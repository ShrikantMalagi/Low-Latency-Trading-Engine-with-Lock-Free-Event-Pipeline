#include "oms.hpp"
#include "exchange.hpp"
#include <algorithm>

namespace hft {
  std::expected<void, OmsError> Oms::submit_new(Order order){
    if (orders.contains(order.order_id)) {
        return std::unexpected(OmsError{
            .code = OmsErrorCode::DuplicateOrderId,
            .message = "order id already tracked",
        });
    }

    orders.emplace(order.order_id, OrderRecord{
        .order = order,
        .status = OrderStatus::PendingNew,
        .original_qty = order.qty,
        .remaining_qty = order.qty,
        .filled_qty = 0,
        .last_fill_price = std::nullopt,
    });
  
    return {};
  }

  std::expected<void, OmsError> Oms::submit_cancel(uint64_t order_id){
    auto it = orders.find(order_id);
    if (it == orders.end()) {
      return std::unexpected(OmsError{
          .code = OmsErrorCode::UnknownOrderId,
          .message = "cancel for unknown order",
      });
    }
  
    auto& rec = it->second;
  
    if (rec.status == OrderStatus::Filled ||
        rec.status == OrderStatus::Canceled ||
        rec.status == OrderStatus::Rejected) {
      return std::unexpected(OmsError{
          .code = OmsErrorCode::AlreadyTerminal,
          .message = "cannot cancel terminal order",
      });
    }
  
    if (rec.status != OrderStatus::Live &&
        rec.status != OrderStatus::PartiallyFilled) {
      return std::unexpected(OmsError{
          .code = OmsErrorCode::InvalidTransition,
          .message = "cancel in invalid state",
      });
    }
  
    rec.status = OrderStatus::PendingCancel;
    return {};
  }

  std::expected<void, OmsError> Oms::on_new_ack(uint64_t order_id){

    auto it = orders.find(order_id);
    if (it == orders.end()) {
      return std::unexpected(OmsError{
          .code = OmsErrorCode::UnknownOrderId,
          .message = "ack for unknown order",
      });
    }

    if (it->second.status != OrderStatus::PendingNew) {
        return std::unexpected(OmsError{
            .code = OmsErrorCode::InvalidTransition,
            .message = "new ack in invalid state",
        });
    }
    
    it->second.status = OrderStatus::Live;
    return {};
  }

  std::expected<void, OmsError> Oms::on_new_reject(uint64_t order_id){

    auto it = orders.find(order_id);
    if (it == orders.end()) {
      return std::unexpected(OmsError{
          .code = OmsErrorCode::UnknownOrderId,
          .message = "new reject for unknown order",
      });
    }

    auto& rec = it->second;

    if (rec.status != OrderStatus::PendingNew) {
      return std::unexpected(OmsError{
          .code = OmsErrorCode::InvalidTransition,
          .message = "new reject in invalid state",
      });
    }

    rec.status = OrderStatus::Rejected;
    return {};
  }

  std::expected<void, OmsError> Oms::on_cancel_ack(uint64_t order_id){

    auto it = orders.find(order_id);
    if (it == orders.end()) {
      return std::unexpected(OmsError{
          .code = OmsErrorCode::UnknownOrderId,
          .message = "cancel ack for unknown order",
      });
    }

    auto& rec = it->second;

    if (rec.status != OrderStatus::PendingCancel) {
      return std::unexpected(OmsError{
          .code = OmsErrorCode::InvalidTransition,
          .message = "cancel ack in invalid state",
      });
    }

    rec.status = OrderStatus::Canceled;
    return {};
  }

  std::expected<void, OmsError> Oms::on_cancel_reject(uint64_t order_id){
    auto it = orders.find(order_id);
    if (it == orders.end()) {
      return std::unexpected(OmsError{
          .code = OmsErrorCode::UnknownOrderId,
          .message = "cancel reject for unknown order",
      });
    }
  
    auto& rec = it->second;
  
    if (rec.status != OrderStatus::PendingCancel) {
      return std::unexpected(OmsError{
          .code = OmsErrorCode::InvalidTransition,
          .message = "cancel reject in invalid state",
      });
    }
  
    if (rec.filled_qty == 0) {
      rec.status = OrderStatus::Live;
    } else {
      rec.status = OrderStatus::PartiallyFilled;
    }
  
    return {};
  }

  std::expected<void, OmsError> Oms::on_fill(uint64_t order_id, int64_t price, int64_t qty){
    
    auto it = orders.find(order_id);
    if (it == orders.end()) {
      return std::unexpected(OmsError{
          .code = OmsErrorCode::UnknownOrderId,
          .message = "fill for unknown order",
      });
    }
  
    auto& rec = it->second;
  
    if (rec.status != OrderStatus::Live && rec.status != OrderStatus::PartiallyFilled) {
      return std::unexpected(OmsError{
          .code = OmsErrorCode::InvalidTransition,
          .message = "fill in invalid state",
      });
    }
  
    if (qty <= 0 || qty > rec.remaining_qty) {
      return std::unexpected(OmsError{
          .code = OmsErrorCode::Overfill,
          .message = "invalid fill quantity",
      });
    }
  
    rec.filled_qty += qty;
    rec.remaining_qty -= qty;
    rec.last_fill_price = price;
  
    if (rec.remaining_qty == 0) {
      rec.status = OrderStatus::Filled;
    } else {
      rec.status = OrderStatus::PartiallyFilled;
    }
  
    return {};
  }

  std::optional<OrderRecord> Oms::get(uint64_t order_id) const{
    auto it = orders.find(order_id);
    if (it == orders.end()) {
      return std::nullopt;
    }
  
    return it->second;
  }

  bool Oms::is_live(uint64_t order_id) const{
    auto it = orders.find(order_id);
    if (it == orders.end()) {
      return false;
    }
  
    const auto status = it->second.status;
    return status == OrderStatus::Live ||
           status == OrderStatus::PartiallyFilled;
  }
}
