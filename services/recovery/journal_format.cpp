#include "journal_format.hpp"

#include <charconv>
#include <cstdint>
#include <fstream>
#include <limits>
#include <sstream>
#include <string_view>
#include <unordered_map>

namespace hft {

namespace {

constexpr std::string_view kJournalMagic = "HFTJ";
constexpr std::string_view kJournalVersion = "1";

uint64_t checksum64(std::string_view data) {
  uint64_t hash = 14695981039346656037ull;
  for (unsigned char c : data) {
    hash ^= static_cast<uint64_t>(c);
    hash *= 1099511628211ull;
  }
  return hash;
}

const char* event_type_name(OmsEventType type) {
  switch (type) {
    case OmsEventType::SubmitNew:
      return "SubmitNew";
    case OmsEventType::NewAck:
      return "NewAck";
    case OmsEventType::NewReject:
      return "NewReject";
    case OmsEventType::SubmitCancel:
      return "SubmitCancel";
    case OmsEventType::CancelAck:
      return "CancelAck";
    case OmsEventType::CancelReject:
      return "CancelReject";
    case OmsEventType::Fill:
      return "Fill";
  }
  return "Unknown";
}

std::expected<OmsEventType, std::string> parse_event_type(std::string_view raw) {
  if (raw == "SubmitNew") return OmsEventType::SubmitNew;
  if (raw == "NewAck") return OmsEventType::NewAck;
  if (raw == "NewReject") return OmsEventType::NewReject;
  if (raw == "SubmitCancel") return OmsEventType::SubmitCancel;
  if (raw == "CancelAck") return OmsEventType::CancelAck;
  if (raw == "CancelReject") return OmsEventType::CancelReject;
  if (raw == "Fill") return OmsEventType::Fill;
  return std::unexpected("unknown event type");
}

std::expected<uint64_t, std::string> parse_u64(std::string_view raw) {
  uint64_t value = 0;
  const char* begin = raw.data();
  const char* end = raw.data() + raw.size();
  const auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc{} || ptr != end) {
    return std::unexpected("invalid unsigned integer");
  }
  return value;
}

std::expected<int64_t, std::string> parse_i64(std::string_view raw) {
  int64_t value = 0;
  const char* begin = raw.data();
  const char* end = raw.data() + raw.size();
  const auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc{} || ptr != end) {
    return std::unexpected("invalid signed integer");
  }
  return value;
}

std::expected<uint8_t, std::string> parse_u8(std::string_view raw) {
  auto parsed = parse_u64(raw);
  if (!parsed) {
    return std::unexpected(parsed.error());
  }
  if (*parsed > std::numeric_limits<uint8_t>::max()) {
    return std::unexpected("uint8 out of range");
  }
  return static_cast<uint8_t>(*parsed);
}

std::string build_payload(const OmsJournalEvent& e) {
  std::ostringstream out;
  out << "type=" << event_type_name(e.type)
      << "|order_id=" << e.order_id
      << "|side=" << static_cast<unsigned>(e.side)
      << "|price=" << e.price
      << "|qty=" << e.qty;
  return out.str();
}

std::unexpected<ReplayError> make_parse_error(
    std::size_t line_number,
    std::string_view message) {
  return std::unexpected(ReplayError{
      .code = ReplayErrorCode::ParseError,
      .line_number = line_number,
      .message = std::string(message),
      .event_type = std::nullopt,
  });
}

std::expected<std::unordered_map<std::string, std::string>, ReplayError> parse_payload_fields(
    std::string_view payload,
    std::size_t line_number) {
  std::unordered_map<std::string, std::string> fields;
  std::size_t start = 0;
  while (start < payload.size()) {
    const std::size_t sep = payload.find('|', start);
    const std::string_view token =
        sep == std::string_view::npos ? payload.substr(start) : payload.substr(start, sep - start);
    if (token.empty()) {
      return make_parse_error(line_number, "empty payload token");
    }
    const std::size_t eq = token.find('=');
    if (eq == std::string_view::npos || eq == 0 || eq + 1 >= token.size()) {
      return make_parse_error(line_number, "invalid payload field");
    }
    const std::string key{token.substr(0, eq)};
    const std::string value{token.substr(eq + 1)};
    if (!fields.emplace(key, value).second) {
      return make_parse_error(line_number, "duplicate payload field");
    }
    if (sep == std::string_view::npos) {
      break;
    }
    start = sep + 1;
  }
  return fields;
}

template <typename ParseFn>
auto parse_required_field(
    const std::unordered_map<std::string, std::string>& fields,
    const char* key,
    std::size_t line_number,
    ParseFn&& parse_fn)
    -> std::expected<typename decltype(parse_fn(std::string_view{}))::value_type, ReplayError> {
  const auto it = fields.find(key);
  if (it == fields.end()) {
    return make_parse_error(line_number, std::string("missing field: ") + key);
  }
  auto parsed = parse_fn(it->second);
  if (!parsed) {
    return make_parse_error(line_number, std::string(key) + ": " + parsed.error());
  }
  return *parsed;
}

}  // namespace

bool append_journal_event(const std::string& path, const OmsJournalEvent& e) {
  std::ofstream out(path, std::ios::app);
  if (!out.is_open()) {
    return false;
  }
  const std::string payload = build_payload(e);
  const uint64_t checksum = checksum64(payload);
  out << kJournalMagic
      << "|version=" << kJournalVersion
      << "|len=" << payload.size()
      << '|'
      << payload
      << "|checksum=" << checksum
      << '\n';
  out.flush();
  return static_cast<bool>(out);
}

std::expected<OmsJournalEvent, ReplayError> parse_journal_line(
    const std::string& line,
    std::size_t line_number) {
  if (!line.starts_with(kJournalMagic)) {
    return make_parse_error(line_number, "missing journal magic");
  }

  const std::string_view view{line};
  const std::string_view version_prefix = "|version=";
  const std::string_view len_prefix = "|len=";
  const std::string_view checksum_prefix = "|checksum=";

  std::size_t cursor = kJournalMagic.size();
  if (cursor >= view.size() || view[cursor] != '|') {
    return make_parse_error(line_number, "missing version separator");
  }
  if (!view.substr(cursor).starts_with(version_prefix)) {
    return make_parse_error(line_number, "missing version field");
  }
  cursor += version_prefix.size();

  const std::size_t version_end = view.find('|', cursor);
  if (version_end == std::string_view::npos) {
    return make_parse_error(line_number, "truncated version field");
  }
  const std::string_view version = view.substr(cursor, version_end - cursor);
  if (version != kJournalVersion) {
    return make_parse_error(line_number, "unsupported journal version");
  }

  cursor = version_end;
  if (!view.substr(cursor).starts_with(len_prefix)) {
    return make_parse_error(line_number, "missing length field");
  }
  cursor += len_prefix.size();

  const std::size_t len_end = view.find('|', cursor);
  if (len_end == std::string_view::npos) {
    return make_parse_error(line_number, "truncated length field");
  }

  auto payload_len = parse_u64(view.substr(cursor, len_end - cursor));
  if (!payload_len) {
    return make_parse_error(line_number, std::string("len: ") + payload_len.error());
  }

  cursor = len_end + 1;
  if (cursor + *payload_len > view.size()) {
    return make_parse_error(line_number, "payload shorter than declared length");
  }

  const std::string_view payload = view.substr(cursor, *payload_len);
  cursor += *payload_len;

  if (!view.substr(cursor).starts_with(checksum_prefix)) {
    return make_parse_error(line_number, "missing checksum field");
  }
  cursor += checksum_prefix.size();

  const std::string_view checksum_text = view.substr(cursor);
  auto checksum = parse_u64(checksum_text);
  if (!checksum) {
    return make_parse_error(line_number, std::string("checksum: ") + checksum.error());
  }
  if (*checksum != checksum64(payload)) {
    return make_parse_error(line_number, "checksum mismatch");
  }

  auto fields = parse_payload_fields(payload, line_number);
  if (!fields) {
    return std::unexpected(fields.error());
  }

  auto type = parse_required_field(*fields, "type", line_number, parse_event_type);
  if (!type) {
    return std::unexpected(type.error());
  }
  auto order_id = parse_required_field(*fields, "order_id", line_number, parse_u64);
  if (!order_id) {
    return std::unexpected(order_id.error());
  }
  auto side = parse_required_field(*fields, "side", line_number, parse_u8);
  if (!side) {
    return std::unexpected(side.error());
  }
  auto price = parse_required_field(*fields, "price", line_number, parse_i64);
  if (!price) {
    return std::unexpected(price.error());
  }
  auto qty = parse_required_field(*fields, "qty", line_number, parse_i64);
  if (!qty) {
    return std::unexpected(qty.error());
  }

  OmsJournalEvent e{};
  e.type = *type;
  e.order_id = *order_id;
  e.side = *side;
  e.price = *price;
  e.qty = *qty;
  return e;
}

}
