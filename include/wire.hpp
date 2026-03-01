#pragma once
#include <cstdint>

namespace wire{
    enum class MsgType: uint16_t{
        NewOrder = 1,
        Cancel = 2,
        Ack = 3,
        Fill = 4,
        Reject = 5
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
    
    #pragma pack(pop)

}
