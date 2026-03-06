#include "order_coordinator.hpp"

namespace hft {

    namespace {

    ExecRejectReason map_oms_error(OmsErrorCode code) {
      switch (code) {
        case OmsErrorCode::DuplicateOrderId: return ExecRejectReason::DuplicateOrderId;
        case OmsErrorCode::UnknownOrderId:   return ExecRejectReason::UnknownOrderId;
        default:                             return ExecRejectReason::InvalidTransition;
      }
    }

    }

    std::expected<ExecResponse, ExecReject> OrderCoordinator::submit_new(Order order) {

        if (order.qty <= 0 || order.price <= 0) {
          ExecReject rej{
              .order_id = order.order_id,
              .reason = ExecRejectReason::InvalidOrder,
              .message = "non-positive qty or price",
          };
          emit(CoordinatorEvent{
            .type = CoordinatorEventType::NewRejected,
            .order_id = order.order_id,
            .reject_reason = rej.reason,
          });
          return std::unexpected(rej);
        }
      
        if (auto r = oms.submit_new(order); !r) {
          ExecReject rej{
              .order_id = order.order_id,
              .reason = map_oms_error(r.error().code),
              .message = r.error().message,
          };
          emit(CoordinatorEvent{
            .type = CoordinatorEventType::NewRejected,
            .order_id = order.order_id,
            .reject_reason = rej.reason,
          });
          return std::unexpected(rej);
        }
      
        auto fills = exchange.add_order(order);
      
        if (auto ack = oms.on_new_ack(order.order_id); !ack) {
          ExecReject rej{
              .order_id = order.order_id,
              .reason = map_oms_error(ack.error().code),
              .message = ack.error().message,
          };
          emit(CoordinatorEvent{
            .type = CoordinatorEventType::InternalError,
            .order_id = order.order_id,
            .reject_reason = rej.reason,
            .message = rej.message,
          });
          return std::unexpected(rej);
        }
        
        emit(CoordinatorEvent{
          .type = CoordinatorEventType::NewAccepted,
          .order_id = order.order_id,
        });

        for (const auto& f : fills) {
          if (auto taker_rec = oms.get(f.taker_order_id); taker_rec.has_value()) {
            auto r = oms.on_fill(f.taker_order_id, f.price, f.qty);
            if (!r) {
              ExecReject rej{
                  .order_id = f.taker_order_id,
                  .reason = map_oms_error(r.error().code),
                  .message = r.error().message,
              };
              emit(CoordinatorEvent{
                .type = CoordinatorEventType::InternalError,
                .order_id = f.taker_order_id,
                .reject_reason = rej.reason,
                .message = rej.message,
              });
              return std::unexpected(rej);
            }
          }
      
          if (auto maker_rec = oms.get(f.maker_order_id); maker_rec.has_value()) {
            auto r = oms.on_fill(f.maker_order_id, f.price, f.qty);
            if (!r) {
              ExecReject rej{
                  .order_id = f.maker_order_id,
                  .reason = map_oms_error(r.error().code),
                  .message = r.error().message,
              };
              emit(CoordinatorEvent{
                .type = CoordinatorEventType::InternalError,
                .order_id = f.maker_order_id,
                .reject_reason = rej.reason,
                .message = rej.message,
              });
              return std::unexpected(rej);
            }
          }
      
          emit(CoordinatorEvent{
              .type = CoordinatorEventType::FillEmitted,
              .order_id = f.taker_order_id,
              .contra_order_id = f.maker_order_id,
              .price = f.price,
              .qty = f.qty,
          });
        }
      
        return ExecResponse{
            .ack = true,
            .fills = std::move(fills),
        };
      }

      std::expected<ExecResponse, ExecReject> OrderCoordinator::submit_cancel(uint64_t order_id) {

        if (auto r = oms.submit_cancel(order_id); !r) {
          ExecReject rej{
              .order_id = order_id,
              .reason = map_oms_error(r.error().code),
              .message = r.error().message,
          };
          emit(CoordinatorEvent{
            .type = CoordinatorEventType::CancelRejected,
            .order_id = order_id,
            .reject_reason = rej.reason,
          });
          return std::unexpected(rej);
        }
      
        const bool canceled = exchange.cancel(order_id);
      
        if (canceled) {
          if (auto ack = oms.on_cancel_ack(order_id); !ack) {
            ExecReject rej{
                .order_id = order_id,
                .reason = map_oms_error(ack.error().code),
                .message = ack.error().message,
            };
            emit(CoordinatorEvent{
              .type = CoordinatorEventType::InternalError,
              .order_id = order_id,
              .reject_reason = rej.reason,
              .message = rej.message,
            });
            return std::unexpected(rej);
          }

          emit(CoordinatorEvent{
            .type = CoordinatorEventType::CancelAccepted,
            .order_id = order_id,
          });
      
          return ExecResponse{
              .ack = true,
              .fills = {},
          };
        }
      
        if (auto rej = oms.on_cancel_reject(order_id); !rej) {
          ExecReject reject{
              .order_id = order_id,
              .reason = map_oms_error(rej.error().code),
              .message = rej.error().message,
          };
          emit(CoordinatorEvent{
            .type = CoordinatorEventType::InternalError,
            .order_id = order_id,
            .reject_reason = reject.reason,
            .message = reject.message,
          });
          return std::unexpected(reject);
        }
      
        ExecReject rej{
            .order_id = order_id,
            .reason = ExecRejectReason::UnknownOrderId,
            .message = "cancel not found on exchange",
        };
        emit(CoordinatorEvent{
          .type = CoordinatorEventType::CancelRejected,
          .order_id = order_id,
          .reject_reason = rej.reason,
        });
        return std::unexpected(rej);
      }   
}
