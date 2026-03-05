#include "exchange.hpp"
#include "order_coordinator.hpp"
#include <algorithm>

namespace hft {

    ExecRejectReason map_oms_error(OmsErrorCode code) {
      switch (code) {
        case OmsErrorCode::DuplicateOrderId: return ExecRejectReason::DuplicateOrderId;
        case OmsErrorCode::UnknownOrderId:   return ExecRejectReason::UnknownOrderId;
        default:                             return ExecRejectReason::InvalidTransition;
      }
    }

    std::expected<ExecResponse, ExecReject> OrderCoordinator::submit_new(Order order) {

        if (order.qty <= 0 || order.price <= 0) {
          return std::unexpected(ExecReject{
              .order_id = order.order_id,
              .reason = ExecRejectReason::InvalidOrder,
              .message = "non-positive qty or price",
          });
        }
      
        if (auto r = oms.submit_new(order); !r) {
          return std::unexpected(ExecReject{
              .order_id = order.order_id,
              .reason = map_oms_error(r.error().code),
              .message = r.error().message,
          });
        }
      
        auto fills = exchange.add_order(order);
      
        if (auto ack = oms.on_new_ack(order.order_id); !ack) {
          return std::unexpected(ExecReject{
              .order_id = order.order_id,
              .reason = map_oms_error(ack.error().code),
              .message = ack.error().message,
          });
        }
      
        for (const auto& f : fills) {
          if (auto taker_rec = oms.get(f.taker_order_id); taker_rec.has_value()) {
            auto r = oms.on_fill(f.taker_order_id, f.price, f.qty);
            if (!r) {
              return std::unexpected(ExecReject{
                  .order_id = f.taker_order_id,
                  .reason = map_oms_error(r.error().code),
                  .message = r.error().message,
              });
            }
          }
        
          if (auto maker_rec = oms.get(f.maker_order_id); maker_rec.has_value()) {
            auto r = oms.on_fill(f.maker_order_id, f.price, f.qty);
            if (!r) {
              return std::unexpected(ExecReject{
                .order_id = f.maker_order_id,
                .reason = map_oms_error(r.error().code),
                .message = r.error().message,
              });
            }
          }
        }

        return ExecResponse{
            .ack = true,
            .fills = std::move(fills),
        };
      }

      std::expected<ExecResponse, ExecReject> OrderCoordinator::submit_cancel(uint64_t order_id) {

        if (auto r = oms.submit_cancel(order_id); !r) {
          return std::unexpected(ExecReject{
              .order_id = order_id,
              .reason = map_oms_error(r.error().code),
              .message = r.error().message,
          });
        }
      
        const bool canceled = exchange.cancel(order_id);
      
        if (canceled) {
          if (auto ack = oms.on_cancel_ack(order_id); !ack) {
            return std::unexpected(ExecReject{
                .order_id = order_id,
                .reason = map_oms_error(ack.error().code),
                .message = ack.error().message,
            });
          }
      
          return ExecResponse{
              .ack = true,
              .fills = {},
          };
        }
      
        if (auto rej = oms.on_cancel_reject(order_id); !rej) {
          return std::unexpected(ExecReject{
              .order_id = order_id,
              .reason = map_oms_error(rej.error().code),
              .message = rej.error().message,
          });
        }
      
        return std::unexpected(ExecReject{
            .order_id = order_id,
            .reason = ExecRejectReason::UnknownOrderId,
            .message = "cancel not found on exchange",
        });
      }   
}
