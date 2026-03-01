#pragma once
#include <expected>
#include <string_view>
#include <cerrno>
#include <cstring>

enum class ErrorCode {
    SocketCreate,
    SetSockOpt,
    Bind,
    Listen,
    Accept,
    Read,
    Write,
    PeerClosed,
    InvalidMessage,
};

struct Error {
    ErrorCode code;
    int sys_errno = 0;
    std::string_view context;
};

inline Error make_sys_error(ErrorCode code, std::string_view context) {
    return Error{
        .code = code,
        .sys_errno = errno,
        .context = context
    };
}
