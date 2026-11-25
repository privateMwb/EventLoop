/**
 * @file            Fd.h
 *
 * @date            2026-11-23
 *
 * @version         0.1.0
 *
 * @copyright       Copyright (c) 2026 privateMwb
 *                  All rights reserved.
 *                  https://github.com/privateMwb/EventLoop
 *
 * @attention       This source is released under the MIT license
 *                  SPDX-License-Identifier: MIT
 *                  <http://opensource.org/licenses/MIT>
 */

#pragma once

// clang-format off
#include <utility> // std::exchange — move construction and release()
// clang-format on

namespace EventLoop {

/**
 * @brief Owning, move-only wrapper around a POSIX file descriptor.
 *
 * Closes the descriptor on destruction. An invalid (empty) wrapper holds -1.
 */
class UniqueFd {
  public:
    /// @brief Constructs an empty wrapper.
    UniqueFd() noexcept = default;

    /// @brief Takes ownership of `fd`. Pass -1 for an empty wrapper.
    explicit UniqueFd(int fd) noexcept : fd_(fd) {}

    ~UniqueFd();

    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;

    UniqueFd(UniqueFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
    UniqueFd& operator=(UniqueFd&& other) noexcept;

    /// @brief The wrapped descriptor, or -1 if empty. Ownership is kept.
    [[nodiscard]] int get() const noexcept {
        return fd_;
    }

    /// @brief True if a descriptor is held.
    [[nodiscard]] bool valid() const noexcept {
        return fd_ >= 0;
    }

    /// @brief Gives up ownership and returns the descriptor without closing it.
    [[nodiscard]] int release() noexcept {
        return std::exchange(fd_, -1);
    }

    /// @brief Closes the held descriptor (if any) and takes ownership of `fd`.
    void reset(int fd = -1) noexcept;

  private:
    int fd_ = -1; ///< The owned descriptor, or -1.
};

/**
 * @brief Puts `fd` into non-blocking mode.
 * @throws std::system_error if `fcntl` fails (for example, on an invalid descriptor).
 */
void setNonBlocking(int fd);

} // namespace EventLoop
