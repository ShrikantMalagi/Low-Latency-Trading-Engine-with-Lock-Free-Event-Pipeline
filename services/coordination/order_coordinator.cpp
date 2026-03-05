#include "exchange.hpp"
#include "order_coordinator.hpp"
#include <algorithm>

namespace hft {

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
              .reason = (r.error().code == OmsErrorCode::DuplicateOrderId)
                            ? ExecRejectReason::DuplicateOrderId
                            : ExecRejectReason::InvalidTransition,
              .message = r.error().message,
          });
        }
      
        auto fills = exchange.add_order(order);
      
        if (auto ack = oms.on_new_ack(order.order_id); !ack) {
          return std::unexpected(ExecReject{
              .order_id = order.order_id,
              .reason = ExecRejectReason::InvalidTransition,
              .message = ack.error().message,
          });
        }
      
        for (const auto& f : fills) {
          auto upd = oms.on_fill(f.taker_order_id, f.price, f.qty);
          if (!upd) {
            return std::unexpected(ExecReject{
                .order_id = f.taker_order_id,
                .reason = ExecRejectReason::InvalidTransition,
                .message = upd.error().message,
            });
          }
        }
      
        return ExecResponse{
            .ack = true,
            .fills = std::move(fills),
            .reject = std::nullopt,
        };
      }

      std::expected<ExecResponse, ExecReject> OrderCoordinator::submit_cancel(uint64_t order_id) {

        if (auto r = oms.submit_cancel(order_id); !r) {
          return std::unexpected(ExecReject{
              .order_id = order_id,
              .reason = (r.error().code == OmsErrorCode::UnknownOrderId)
                            ? ExecRejectReason::UnknownOrderId
                            : ExecRejectReason::InvalidTransition,
              .message = r.error().message,
          });
        }
      
        const bool canceled = exchange.cancel(order_id);
      
        if (canceled) {
          if (auto ack = oms.on_cancel_ack(order_id); !ack) {
            return std::unexpected(ExecReject{
                .order_id = order_id,
                .reason = ExecRejectReason::InvalidTransition,
                .message = ack.error().message,
            });
          }
      
          return ExecResponse{
              .ack = true,
              .fills = {},
              .reject = std::nullopt,
          };
        }
      
        if (auto rej = oms.on_cancel_reject(order_id); !rej) {
          return std::unexpected(ExecReject{
              .order_id = order_id,
              .reason = ExecRejectReason::InvalidTransition,
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