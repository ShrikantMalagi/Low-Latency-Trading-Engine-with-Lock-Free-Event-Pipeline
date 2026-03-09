#include "order_coordinator.hpp"
#include "recovery.hpp"

namespace hft {

namespace {

ExecRejectReason map_oms_error(OmsErrorCode code) {
  switch (code) {
    case OmsErrorCode::DuplicateOrderId: return ExecRejectReason::DuplicateOrderId;
    case OmsErrorCode::UnknownOrderId:   return ExecRejectReason::UnknownOrderId;
    default:                             return ExecRejectReason::InvalidTransition;
  }
}

std::expected<void, ExecReject> append_or_reject(
    const std::string& journal_path,
    const OmsJournalEvent& event,
    uint64_t order_id,
    std::string_view context) {
  if (journal_path.empty() || append_journal_event(journal_path, event)) {
    return {};
  }
  return std::unexpected(ExecReject{
      .order_id = order_id,
      .reason = ExecRejectReason::InvalidTransition,
      .message = context,
  });
}

}

std::expected<ExecResponse, ExecReject> OrderCoordinator::submit_new(Order order) {
  auto reject_new = [&](ExecRejectReason reason, std::string_view message) -> std::expected<ExecResponse, ExecReject> {
    ExecReject rej{
        .order_id = order.order_id,
        .reason = reason,
        .message = message,
    };
    emit(CoordinatorEvent{
        .type = CoordinatorEventType::NewRejected,
        .order_id = order.order_id,
        .reject_reason = rej.reason,
        .message = rej.message,
    });
    return std::unexpected(rej);
  };

  auto internal_error = [&](uint64_t order_id, ExecRejectReason reason, std::string_view message) -> std::expected<ExecResponse, ExecReject> {
    ExecReject rej{
        .order_id = order_id,
        .reason = reason,
        .message = message,
    };
    emit(CoordinatorEvent{
        .type = CoordinatorEventType::InternalError,
        .order_id = order_id,
        .reject_reason = rej.reason,
        .message = rej.message,
    });
    return std::unexpected(rej);
  };

  auto append_or_internal = [&](const OmsJournalEvent& event, uint64_t order_id, std::string_view context) -> std::optional<ExecReject> {
    if (auto jr = append_or_reject(journal_path, event, order_id, context); !jr) {
      ExecReject rej = jr.error();
      emit(CoordinatorEvent{
          .type = CoordinatorEventType::InternalError,
          .order_id = rej.order_id,
          .reject_reason = rej.reason,
          .message = rej.message,
      });
      return rej;
    }
    return std::nullopt;
  };

  if (order.qty <= 0 || order.price <= 0) {
    return reject_new(ExecRejectReason::InvalidOrder, "non-positive qty or price");
  }

  if (auto r = oms.submit_new(order); !r) {
    return reject_new(map_oms_error(r.error().code), r.error().message);
  }

  if (auto rej = append_or_internal(
          OmsJournalEvent{
              .type = OmsEventType::SubmitNew,
              .order_id = order.order_id,
              .side = static_cast<uint8_t>(order.side),
              .price = order.price,
              .qty = order.qty,
          },
          order.order_id,
          "journal append failed after submit_new");
      rej.has_value()) {
    return std::unexpected(*rej);
  }

  auto fills = exchange.add_order(order);

  if (auto ack = oms.on_new_ack(order.order_id); !ack) {
    return internal_error(order.order_id, map_oms_error(ack.error().code), ack.error().message);
  }

  if (auto rej = append_or_internal(
          OmsJournalEvent{
              .type = OmsEventType::NewAck,
              .order_id = order.order_id,
              .side = static_cast<uint8_t>(order.side),
              .price = order.price,
              .qty = order.qty,
          },
          order.order_id,
          "journal append failed after on_new_ack");
      rej.has_value()) {
    return std::unexpected(*rej);
  }

  emit(CoordinatorEvent{
      .type = CoordinatorEventType::NewAccepted,
      .order_id = order.order_id,
  });

  for (const auto& f : fills) {
    if (oms.get(f.taker_order_id).has_value()) {
      if (auto r = oms.on_fill(f.taker_order_id, f.price, f.qty); !r) {
        return internal_error(f.taker_order_id, map_oms_error(r.error().code), r.error().message);
      }
      if (auto rej = append_or_internal(
              OmsJournalEvent{
                  .type = OmsEventType::Fill,
                  .order_id = f.taker_order_id,
                  .side = 0,
                  .price = f.price,
                  .qty = f.qty,
              },
              f.taker_order_id,
              "journal append failed after taker fill");
          rej.has_value()) {
        return std::unexpected(*rej);
      }
    }

    if (oms.get(f.maker_order_id).has_value()) {
      if (auto r = oms.on_fill(f.maker_order_id, f.price, f.qty); !r) {
        return internal_error(f.maker_order_id, map_oms_error(r.error().code), r.error().message);
      }
      if (auto rej = append_or_internal(
              OmsJournalEvent{
                  .type = OmsEventType::Fill,
                  .order_id = f.maker_order_id,
                  .side = 0,
                  .price = f.price,
                  .qty = f.qty,
              },
              f.maker_order_id,
              "journal append failed after maker fill");
          rej.has_value()) {
        return std::unexpected(*rej);
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
  auto reject_cancel = [&](ExecRejectReason reason, std::string_view message) -> std::expected<ExecResponse, ExecReject> {
    ExecReject rej{
        .order_id = order_id,
        .reason = reason,
        .message = message,
    };
    emit(CoordinatorEvent{
        .type = CoordinatorEventType::CancelRejected,
        .order_id = order_id,
        .reject_reason = rej.reason,
        .message = rej.message,
    });
    return std::unexpected(rej);
  };

  auto internal_error = [&](ExecRejectReason reason, std::string_view message) -> std::expected<ExecResponse, ExecReject> {
    ExecReject rej{
        .order_id = order_id,
        .reason = reason,
        .message = message,
    };
    emit(CoordinatorEvent{
        .type = CoordinatorEventType::InternalError,
        .order_id = order_id,
        .reject_reason = rej.reason,
        .message = rej.message,
    });
    return std::unexpected(rej);
  };

  auto append_or_internal = [&](const OmsJournalEvent& event, std::string_view context) -> std::optional<ExecReject> {
    if (auto jr = append_or_reject(journal_path, event, order_id, context); !jr) {
      ExecReject rej = jr.error();
      emit(CoordinatorEvent{
          .type = CoordinatorEventType::InternalError,
          .order_id = rej.order_id,
          .reject_reason = rej.reason,
          .message = rej.message,
      });
      return rej;
    }
    return std::nullopt;
  };

  if (auto r = oms.submit_cancel(order_id); !r) {
    return reject_cancel(map_oms_error(r.error().code), r.error().message);
  }

  if (auto rej = append_or_internal(
          OmsJournalEvent{
              .type = OmsEventType::SubmitCancel,
              .order_id = order_id,
              .side = 0,
              .price = 0,
              .qty = 0,
          },
          "journal append failed after submit_cancel");
      rej.has_value()) {
    return std::unexpected(*rej);
  }

  if (exchange.cancel(order_id)) {
    if (auto ack = oms.on_cancel_ack(order_id); !ack) {
      return internal_error(map_oms_error(ack.error().code), ack.error().message);
    }

    if (auto rej = append_or_internal(
            OmsJournalEvent{
                .type = OmsEventType::CancelAck,
                .order_id = order_id,
                .side = 0,
                .price = 0,
                .qty = 0,
            },
            "journal append failed after on_cancel_ack");
        rej.has_value()) {
      return std::unexpected(*rej);
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
    return internal_error(map_oms_error(rej.error().code), rej.error().message);
  }

  if (auto rej = append_or_internal(
          OmsJournalEvent{
              .type = OmsEventType::CancelReject,
              .order_id = order_id,
              .side = 0,
              .price = 0,
              .qty = 0,
          },
          "journal append failed after on_cancel_reject");
      rej.has_value()) {
    return std::unexpected(*rej);
  }

  return reject_cancel(ExecRejectReason::UnknownOrderId, "cancel not found on exchange");
}

}
