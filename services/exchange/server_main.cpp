#include "exchange.hpp"
#include "wire.hpp"
#include "error.hpp"

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <expected>
#include <cstdio>


namespace{

    enum class RejectReason : uint16_t {
        InvalidMessage = 1,
        InvalidSide = 2,
        CancelNotFound = 3,
        InvalidQuantity = 4,
        InvalidPrice = 5,
        DuplicateOrderId = 6,
    };

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

    std::expected<void, hft::Error> handle_new_order(
        int client_fd,
        uint16_t seq,
        hft::Exchange& exchange
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

        if (exchange.has_order(msg.order_id)) {
            return send_reject(client_fd, seq, msg.order_id, RejectReason::DuplicateOrderId);
        }        

        hft::Order order{
            .order_id = msg.order_id,
            .side = (msg.side == static_cast<uint8_t>(hft::Side::Buy)) ? hft::Side::Buy : hft::Side::Sell,
            .price = msg.price,
            .qty = msg.qty,
        };
        
        auto fills = exchange.add_order(order);

        if (auto result = send_ack(client_fd, seq, order.order_id); !result.has_value()){
            return result;
        }

        for (const auto& fill : fills) {
            if (auto result = send_fill(client_fd, seq, fill); !result) {
                return result;
            }
        }
    
        return {};
    }

    std::expected<void, hft::Error> handle_cancel(
        int client_fd,
        uint16_t seq,
        hft::Exchange& exchange
    ) {
        wire::Cancel msg{};
        if (auto result = read_exact(client_fd, &msg, sizeof(msg)); !result) {
            return result;
        }
    
        if (exchange.cancel(msg.order_id)) {
            return send_ack(client_fd, seq, msg.order_id);
        }
    
        return send_reject(client_fd, seq, msg.order_id, RejectReason::CancelNotFound);
    }
    
    std::expected<void, hft::Error> handle_client(int client_fd, hft::Exchange& exchange) {
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
    
                    if (auto result = handle_new_order(client_fd, header.seq, exchange); !result) {
                        return result;
                    }
                    break;
    
                case wire::MsgType::Cancel:
                    if (header.length != sizeof(wire::Cancel)) {
                        return send_reject(client_fd, header.seq, 0, RejectReason::InvalidMessage);
                    }
    
                    if (auto result = handle_cancel(client_fd, header.seq, exchange); !result) {
                        return result;
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


}

int main()
{   

    constexpr int port = 9000;

    auto listen_result = make_listen_socket(port);
    if (!listen_result) {
        print_error(listen_result.error(), "fatal");
        return 1;
    }

    int listen_fd = *listen_result;
    hft::Exchange exchange;
    
    while (true) {
        auto client_result = accept_client(listen_fd);
        if (!client_result) {
            print_error(client_result.error(), "accept");
            continue;
        }
    
        int client_fd = *client_result;
    
        auto session_result = handle_client(client_fd, exchange);
        if (!session_result && session_result.error().code != hft::ErrorCode::PeerClosed) {
            print_error(session_result.error(), "client");
        }
    
        ::close(client_fd);
    }
    
    ::close(listen_fd);
    return 0;
}
