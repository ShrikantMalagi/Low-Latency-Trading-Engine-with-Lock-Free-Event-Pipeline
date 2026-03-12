#include "exchange.hpp"
#include "wire.hpp"
#include "error.hpp"
#include "order_coordinator.hpp"
#include "coordinator_event_sink.hpp"
#include "coordinator_metrics.hpp"
#include "recovery.hpp"
#include "journal_sink.hpp"

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <csignal>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <expected>
#include <memory>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <string_view>


namespace{

    volatile std::sig_atomic_t g_shutdown_requested = 0;

    void handle_shutdown_signal(int) {
        g_shutdown_requested = 1;
    }

    bool shutdown_requested() {
        return g_shutdown_requested != 0;
    }

    void install_signal_handlers() {
        std::signal(SIGINT, handle_shutdown_signal);
        std::signal(SIGTERM, handle_shutdown_signal);
    }

    const char* find_arg_value(int argc, char* argv[], std::string_view prefix) {
        if (argv == nullptr) {
            return nullptr;
        }
        for (int i = 1; i < argc; ++i) {
            if (argv[i] == nullptr) {
                continue;
            }
            std::string_view arg{argv[i]};
            if (arg.starts_with(prefix)) {
                return argv[i] + static_cast<std::ptrdiff_t>(prefix.size());
            }
        }
        return nullptr;
    }

    enum class RejectReason : uint16_t {
        InvalidMessage = 1,
        InvalidSide = 2,
        CancelNotFound = 3,
        InvalidQuantity = 4,
        InvalidPrice = 5,
        DuplicateOrderId = 6,
        JournalBackpressure = 7,
    };

    RejectReason to_wire_reject_reason(const hft::ExecReject& reject) {
        if (reject.reason == hft::ExecRejectReason::InvalidTransition &&
            reject.message == "journal backpressure") {
            return RejectReason::JournalBackpressure;
        }

        const auto reason = reject.reason;
        switch (reason) {
            case hft::ExecRejectReason::DuplicateOrderId:
            return RejectReason::DuplicateOrderId;
            case hft::ExecRejectReason::UnknownOrderId:
                return RejectReason::CancelNotFound;
            case hft::ExecRejectReason::InvalidTransition:
                return RejectReason::InvalidMessage;
            case hft::ExecRejectReason::InvalidOrder:
            default:
                return RejectReason::InvalidMessage;
        }
    }

    std::size_t parse_event_sink_max_queue_size(int argc, char* argv[]) {
        constexpr std::size_t kDefaultMaxQueueSize = 4096;
        const char* raw = find_arg_value(argc, argv, "--event-sink-max-queue=");
        if (raw == nullptr || raw[0] == '\0') {
            return kDefaultMaxQueueSize;
        }

        char* end = nullptr;
        const unsigned long long parsed = std::strtoull(raw, &end, 10);
        if (end == raw || *end != '\0' || parsed == 0) {
            return kDefaultMaxQueueSize;
        }

        return static_cast<std::size_t>(parsed);
    }

    std::string parse_journal_path(int argc, char* argv[]) {
        constexpr std::string_view kDefaultJournalPath = "state/oms.journal";
        const char* raw = find_arg_value(argc, argv, "--journal-path=");
        if (raw == nullptr || raw[0] == '\0') {
            return std::string(kDefaultJournalPath);
        }
        return std::string(raw);
    }

    bool use_sync_journal(int argc, char* argv[]) {
        const char* raw = find_arg_value(argc, argv, "--journal-mode=");
        if (raw == nullptr || raw[0] == '\0') {
            return false;
        }
        return std::string_view(raw) == "sync";
    }

    std::size_t parse_journal_queue_capacity(int argc, char* argv[]) {
        constexpr std::size_t kDefaultJournalQueueCapacity = 16384;
        const char* raw = find_arg_value(argc, argv, "--journal-queue-capacity=");
        if (raw == nullptr || raw[0] == '\0') {
            return kDefaultJournalQueueCapacity;
        }

        char* end = nullptr;
        const unsigned long long parsed = std::strtoull(raw, &end, 10);
        if (end == raw || *end != '\0' || parsed == 0) {
            return kDefaultJournalQueueCapacity;
        }

        return static_cast<std::size_t>(parsed);
    }

    std::expected<int, hft::Error> make_listen_socket(int port) {
        int server_socket = ::socket(AF_INET, SOCK_STREAM, 0);
        if (server_socket < 0) {
            return std::unexpected(hft::make_sys_error(hft::ErrorCode::SocketCreate, "socket"));
        }
    
        int yes = 1;
        if (::setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
            ::close(server_socket);
            return std::unexpected(hft::make_sys_error(hft::ErrorCode::SetSockOpt, "setsockopt"));
        }
    
        sockaddr_in server_address{};
        server_address.sin_family = AF_INET;
        server_address.sin_port = htons(static_cast<uint16_t>(port));
        server_address.sin_addr.s_addr = htonl(INADDR_ANY);
    
        if (::bind(server_socket, reinterpret_cast<sockaddr*>(&server_address), sizeof(server_address)) < 0) {
            ::close(server_socket);
            return std::unexpected(hft::make_sys_error(hft::ErrorCode::Bind, "bind"));
        }
    
        if (::listen(server_socket, 16) < 0) {
            ::close(server_socket);
            return std::unexpected(hft::make_sys_error(hft::ErrorCode::Listen, "listen"));
        }
    
        return server_socket;
    }
    

    std::expected<int,hft::Error> accept_client(int server_socket){
        while(true){
            int client_fd = ::accept(server_socket,nullptr,nullptr);
            if(client_fd >= 0){
                return client_fd;
            }
            
            if( errno == EINTR){
                if (shutdown_requested()) {
                    return std::unexpected(hft::Error{
                        .code = hft::ErrorCode::PeerClosed,
                        .sys_errno = 0,
                        .context = "shutdown requested",
                    });
                }
                continue;
            }

            return std::unexpected(hft::make_sys_error(hft::ErrorCode::Accept, "accept"));  
        }
    }

    std::expected<void, hft::Error> read_exact(int fd, void* buf, size_t n) {
        auto* out = static_cast<char*>(buf);
        size_t total = 0;
    
        while (total < n) {
            ssize_t r = ::read(fd, out + total, n - total);
    
            if (r == 0) {
                return std::unexpected(hft::Error{
                    .code = hft::ErrorCode::PeerClosed,
                    .sys_errno = 0,
                    .context = "peer closed",
                });
            }
    
            if (r < 0) {
                if (errno == EINTR) {
                    if (shutdown_requested()) {
                        return std::unexpected(hft::Error{
                            .code = hft::ErrorCode::PeerClosed,
                            .sys_errno = 0,
                            .context = "shutdown requested",
                        });
                    }
                    continue;
                }
    
                return std::unexpected(hft::make_sys_error(hft::ErrorCode::Read, "read"));
            }
    
            total += static_cast<size_t>(r);
        }
    
        return {};
    }
    

    std::expected<void, hft::Error> write_all(int fd, const void* buf, size_t n) {
        const auto* in = static_cast<const char*>(buf);
        size_t total = 0;
    
        while (total < n) {
            ssize_t w = ::write(fd, in + total, n - total);
    
            if (w < 0) {
                if (errno == EINTR) {
                    if (shutdown_requested()) {
                        return std::unexpected(hft::Error{
                            .code = hft::ErrorCode::Write,
                            .sys_errno = 0,
                            .context = "shutdown requested",
                        });
                    }
                    continue;
                }
    
                return std::unexpected(hft::make_sys_error(hft::ErrorCode::Write, "write"));
            }
            
            if (w == 0) {
                return std::unexpected(hft::Error{
                    .code = hft::ErrorCode::Write,
                    .sys_errno = 0,
                    .context = "write returned 0",
                });
            }
            
    
            total += static_cast<size_t>(w);
        }
    
        return {};
    }
    

    std::expected<void, hft::Error> send_frame(
        int fd,
        wire::MsgType type,
        uint16_t seq,
        const void* payload,
        uint16_t payload_len
    ) {
        wire::Header header{
            .type = static_cast<uint16_t>(type),
            .length = payload_len,
            .seq = seq,
        };
    
        if (auto result = write_all(fd, &header, sizeof(header)); !result.has_value()) {
            return result;
        }
    
        if (payload_len == 0) {
            return {};
        }
    
        return write_all(fd, payload, payload_len);
    }
    

    std::expected<void, hft::Error> send_ack(int fd, uint16_t seq, uint64_t order_id) {
        wire::Ack ack{
            .order_id = order_id,
        };
    
        return send_frame(fd, wire::MsgType::Ack, seq, &ack, sizeof(ack));
    }
    
    std::expected<void, hft::Error> send_fill(int fd, uint16_t seq, const hft::Fill& fill) {
        wire::Fill msg{
            .order_id = fill.taker_order_id,
            .price = fill.price,
            .qty = fill.qty,
        };
    
        return send_frame(fd, wire::MsgType::Fill, seq, &msg, sizeof(msg));
    }
    
    std::expected<void, hft::Error> send_reject(
        int fd,
        uint16_t seq,
        uint64_t order_id,
        RejectReason reason
    ) {
        wire::Reject reject{
            .order_id = order_id,
            .reason = static_cast<uint16_t>(reason),
        };
    
        return send_frame(fd, wire::MsgType::Reject, seq, &reject, sizeof(reject));
    }

    std::expected<void, hft::Error> handle_get_metrics(
        int client_fd,
        uint16_t seq,
        const hft::CoordinatorMetrics& metrics,
        const hft::QueueEventSink& event_sink,
        const hft::JournalSink* journal_sink,
        const hft::RecoveryStatus& recovery_status
    );

    std::expected<void, hft::Error> handle_new_order(
        int client_fd,
        uint16_t seq,
        hft::OrderCoordinator& coordinator
    ) {
        wire::NewOrder msg{};

        if(auto result = read_exact(client_fd, &msg, sizeof(msg)); !result.has_value()){
            return result;
        }

        if(msg.side > static_cast<uint8_t>(hft::Side::Sell)){
            return send_reject(client_fd, seq, msg.order_id, RejectReason::InvalidSide);
        }

        if (msg.qty <= 0) {
            return send_reject(client_fd, seq, msg.order_id, RejectReason::InvalidQuantity);
        }

        if (msg.price <= 0) {
            return send_reject(client_fd, seq, msg.order_id, RejectReason::InvalidPrice);
        }     

        hft::Order order{
            .order_id = msg.order_id,
            .side = (msg.side == static_cast<uint8_t>(hft::Side::Buy)) ? hft::Side::Buy : hft::Side::Sell,
            .price = msg.price,
            .qty = msg.qty,
        };
    
        auto exec = coordinator.submit_new(order);
        if (!exec) {
            const auto& rej = exec.error();
            return send_reject(
                client_fd,
                seq,
                rej.order_id,
                to_wire_reject_reason(rej)
            );
        }
    
        const auto& response = *exec;
    
        if (response.ack) {
            if (auto ack = send_ack(client_fd, seq, order.order_id); !ack) {
                return ack;
            }
        }
    
        for (const auto& f : response.fills) {
            if (auto sf = send_fill(client_fd, seq, f); !sf) {
                return sf;
            }
        }
    
        return {};
    }

    std::expected<void, hft::Error> handle_cancel(
        int client_fd,
        uint16_t seq,
        hft::OrderCoordinator& coordinator
    ) {
        wire::Cancel msg{};
        if (auto result = read_exact(client_fd, &msg, sizeof(msg)); !result) {
            return result;
        }
    
        auto exec = coordinator.submit_cancel(msg.order_id);
        if (!exec) {
            const auto& rej = exec.error();
            return send_reject(
                client_fd,
                seq,
                rej.order_id,
                to_wire_reject_reason(rej)
            );
        }

        const auto& response = *exec;
        if (response.ack) {
            return send_ack(client_fd, seq, msg.order_id);
        }

        return send_reject(client_fd, seq, msg.order_id, RejectReason::InvalidMessage);
    }
    
    std::expected<void, hft::Error> handle_client(
        int client_fd,
        hft::OrderCoordinator& coordinator,
        const hft::CoordinatorMetrics& metrics,
        const hft::QueueEventSink& event_sink,
        const hft::JournalSink* journal_sink,
        const hft::RecoveryStatus& recovery_status
    ) {
        while (true) {
            wire::Header header{};
            if (auto result = read_exact(client_fd, &header, sizeof(header)); !result) {
                return result;
            }
    
            auto type = static_cast<wire::MsgType>(header.type);
    
            switch (type) {
                case wire::MsgType::NewOrder:
                    if (header.length != sizeof(wire::NewOrder)) {
                        return send_reject(client_fd, header.seq, 0, RejectReason::InvalidMessage);
                    }
    
                    if (auto result = handle_new_order(client_fd, header.seq, coordinator); !result) {
                        return result;
                    }
                    break;
    
                case wire::MsgType::Cancel:
                    if (header.length != sizeof(wire::Cancel)) {
                        return send_reject(client_fd, header.seq, 0, RejectReason::InvalidMessage);
                    }
    
                    if (auto result = handle_cancel(client_fd, header.seq, coordinator); !result) {
                        return result;
                    }
                    break;

                case wire::MsgType::GetMetrics:
                    if (header.length != 0) {
                      return send_reject(client_fd, header.seq, 0, RejectReason::InvalidMessage);
                    }
                    if (auto r = handle_get_metrics(client_fd, header.seq, metrics, event_sink, journal_sink, recovery_status); !r) {
                      return r;
                    }
                    break;
    
                default:
                    return send_reject(client_fd, header.seq, 0, RejectReason::InvalidMessage);
            }
        }
    }
    
    void print_error(const hft::Error& error, const char* prefix) {
        std::fprintf(
            stderr,
            "%s: %.*s (%d: %s)\n",
            prefix,
            static_cast<int>(error.context.size()),
            error.context.data(),
            error.sys_errno,
            error.sys_errno ? std::strerror(error.sys_errno) : "n/a"
        );
    }

    wire::MetricsSnapshot to_wire_metrics(const hft::CoordinatorMetricsSnapshot& s) {
        wire::MetricsSnapshot out{};
        for (std::size_t i = 0; i < 6; ++i) out.event_type_counts[i] = s.event_type_counts[i];
        for (std::size_t i = 0; i < 4; ++i) out.reject_reason_counts[i] = s.reject_reason_counts[i];
        out.coordinator_dropped_events = s.coordinator_dropped_events;
        out.coordinator_queued_events = s.coordinator_queued_events;
        out.journal_enqueued_events = s.journal_enqueued_events;
        out.journal_flushed_events = s.journal_flushed_events;
        out.journal_backpressure_events = s.journal_backpressure_events;
        out.journal_queue_depth = s.journal_queue_depth;
        out.recovery_replay_attempted = s.recovery_replay_attempted;
        out.recovery_replay_succeeded = s.recovery_replay_succeeded;
        out.recovery_records_replayed = s.recovery_records_replayed;
        out.recovery_error_code = s.recovery_error_code;
        out.recovery_error_line = s.recovery_error_line;
        return out;
    }

    std::expected<void, hft::Error> handle_get_metrics(
        int client_fd,
        uint16_t seq,
        const hft::CoordinatorMetrics& metrics,
        const hft::QueueEventSink& event_sink,
        const hft::JournalSink* journal_sink,
        const hft::RecoveryStatus& recovery_status) {
      const auto snap = hft::snapshot(metrics, event_sink, journal_sink, &recovery_status);
      const auto msg = to_wire_metrics(snap);
      return send_frame(client_fd, wire::MsgType::MetricsSnapshot, seq, &msg, sizeof(msg));
    }
}

int main(int argc, char* argv[])
{   
    constexpr int port = 9000;
    install_signal_handlers();

    auto listen_result = make_listen_socket(port);
    if (!listen_result) {
        print_error(listen_result.error(), "fatal");
        return 1;
    }

    int listen_fd = *listen_result;
    hft::Exchange exchange;
    hft::Oms oms;
    hft::RecoveryStatus recovery_status{};

    
    const std::string journal_path = parse_journal_path(argc, argv);

    std::unique_ptr<hft::JournalSink> journal_sink;
    if (use_sync_journal(argc, argv)) {
        journal_sink = std::make_unique<hft::SyncJournalSink>(journal_path);
    } else {
        journal_sink = std::make_unique<hft::AsyncJournalSink>(
            journal_path,
            parse_journal_queue_capacity(argc, argv),
            hft::BackpressurePolicy::FailFast);
    }
    recovery_status.replay_attempted = true;
    const auto replay_result = hft::replay_journal(journal_path, oms);
    if (!replay_result) {
        recovery_status.replay_succeeded = false;
        recovery_status.records_replayed = replay_result.error().records_replayed;
        recovery_status.replay_error_code = static_cast<uint64_t>(replay_result.error().code);
        recovery_status.replay_error_line = replay_result.error().line_number;
        std::fprintf(
            stderr,
            "fatal: replay_journal failed line=%zu code=%u records=%llu message=%s\n",
            replay_result.error().line_number,
            static_cast<unsigned>(replay_result.error().code),
            static_cast<unsigned long long>(replay_result.error().records_replayed),
            replay_result.error().message.c_str());
        return 1;
    }
    recovery_status.replay_succeeded = true;
    recovery_status.records_replayed = replay_result->records_replayed;
    hft::rebuild_exchange_from_oms(oms, exchange);

    hft::QueueEventSink event_sink(parse_event_sink_max_queue_size(argc, argv));
    hft::OrderCoordinator coordinator(oms, exchange, &event_sink, journal_sink.get());
    hft::CoordinatorMetrics coordinator_metrics{};

    
    while (!shutdown_requested()) {
        auto client_result = accept_client(listen_fd);
        if (!client_result) {
            if (shutdown_requested() || client_result.error().context == "shutdown requested") {
                break;
            }
            print_error(client_result.error(), "accept");
            continue;
        }
    
        int client_fd = *client_result;
    
        auto session_result = handle_client(
            client_fd,
            coordinator,
            coordinator_metrics,
            event_sink,
            journal_sink.get(),
            recovery_status);
        if (!session_result &&
            session_result.error().code != hft::ErrorCode::PeerClosed &&
            session_result.error().context != "shutdown requested") {
            print_error(session_result.error(), "client");
        }

        hft::drain_and_report(event_sink, coordinator_metrics);
    
        ::close(client_fd);
    }

    journal_sink->flush();
    
    ::close(listen_fd);
    return 0;
}
