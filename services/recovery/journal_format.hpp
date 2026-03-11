#pragma once

#include "recovery.hpp"

#include <expected>
#include <string>

namespace hft {

bool append_journal_event(const std::string& path, const OmsJournalEvent& e);

std::expected<OmsJournalEvent, ReplayError> parse_journal_line(
    const std::string& line,
    std::size_t line_number);

}
