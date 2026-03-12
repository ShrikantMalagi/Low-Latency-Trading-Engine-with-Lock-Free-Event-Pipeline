#pragma once
#include <cstdint>

namespace wire{
    enum class MsgType: uint16_t{
        NewOrder = 1,
        Cancel = 2,
        Ack = 3,
        Fill = 4,
        Reject = 5,
        GetMetrics = 100,
        MetricsSnapshot = 101
    };
    
    #pragma pack(push, 1)

    struct Header{
        uint16_t type;
        uint16_t length;
        uint16_t seq;
    };

    struct NewOrder {
        uint64_t order_id;
        uint8_t  side;
        int64_t  price;
        int64_t  qty;
    };

    struct Cancel {
        uint64_t order_id;
    };
    
    struct Ack {
        uint64_t order_id;
    };
    
    struct Fill {
        uint64_t order_id;
        int64_t  price;
        int64_t  qty;
    };

    struct Reject {
        uint64_t order_id;
        uint16_t reason;
    };

    struct MetricsSnapshot {
        uint64_t event_type_counts[6];
        uint64_t reject_reason_counts[4];
        uint64_t coordinator_dropped_events;
        uint64_t coordinator_queued_events;
        uint64_t journal_enqueued_events;
        uint64_t journal_flushed_events;
        uint64_t journal_backpressure_events;
        uint64_t journal_queue_depth;
        uint64_t recovery_replay_attempted;
        uint64_t recovery_replay_succeeded;
        uint64_t recovery_records_replayed;
        uint64_t recovery_error_code;
        uint64_t recovery_error_line;
    };
    
    #pragma pack(pop)

}
