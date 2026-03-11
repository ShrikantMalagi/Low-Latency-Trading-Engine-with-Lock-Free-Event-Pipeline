#pragma once

#include "recovery.hpp"

#include <expected>
#include <string>

namespace hft {

std::expected<ReplayStats, ReplayError> replay_journal(const std::string& path, Oms& oms);

}
