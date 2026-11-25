/**
 * @file Fd.cpp
 * @brief UniqueFd and descriptor helpers.
 */

// ============================================================
// Implementation for EventLoop::UniqueFd and setNonBlocking.
// ============================================================
//
//  Sections:
//   1. UniqueFd
//   2. Descriptor Helpers
//
// ============================================================

// clang-format off
#include <EventLoop/Fd.h> // UniqueFd, setNonBlocking — what this file implements

#include <cerrno>         // errno — captured when fcntl fails
#include <fcntl.h>        // fcntl, F_GETFL, F_SETFL, O_NONBLOCK — non-blocking mode
#include <system_error>   // std::system_error, std::generic_category — failure reporting
#include <unistd.h>       // ::close — releasing the descriptor
// clang-format on

namespace EventLoop {

// ============================================================
// Section 1 — UniqueFd
// ============================================================

UniqueFd::~UniqueFd() {
    reset();
}

UniqueFd& UniqueFd::operator=(UniqueFd&& other) noexcept {
    if (this != &other) {
        reset(other.release());
    }
    return *this;
}

void UniqueFd::reset(int fd) noexcept {
    if (fd_ >= 0) {
        // Linux releases the descriptor even when close() reports EINTR, so
        // retrying would risk closing an unrelated, freshly reused one.
        ::close(fd_);
    }
    fd_ = fd;
}

// ============================================================
// Section 2 — Descriptor Helpers
// ============================================================

void setNonBlocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        throw std::system_error(errno, std::generic_category(), "setNonBlocking");
    }
}

} // namespace EventLoop
