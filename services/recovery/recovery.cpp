#include "recovery.hpp"

namespace hft {

void rebuild_exchange_from_oms(const Oms& oms, Exchange& exchange) {
  for (const auto& rec : oms.all_orders()) {
    if (rec.status == OrderStatus::Live || rec.status == OrderStatus::PartiallyFilled) {
      Order o = rec.order;
      o.qty = rec.remaining_qty;
      if (o.qty > 0) {
        (void)exchange.add_order(o);
      }
    }
  }
}

}
