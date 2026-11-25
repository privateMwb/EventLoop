/**
 * @file            Endpoint.h
 *
 * @date            2026-11-24
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
#include <cstdint>      // std::uint16_t — port type
#include <netinet/in.h> // sockaddr_in, sockaddr_in6 — address layouts
#include <string>       // std::string — textual form
#include <string_view>  // std::string_view — numeric address input
#include <sys/socket.h> // sockaddr, sockaddr_storage, socklen_t — generic socket address
// clang-format on

namespace EventLoop {

/**
 * @brief A numeric IPv4 or IPv6 socket address plus port.
 *
 * Holds an address ready to hand to `bind`/`connect`. Name resolution is out
 * of scope: construction takes numeric addresses only.
 */
class Endpoint {
  public:
    /// @brief Constructs an empty endpoint (`family()` is AF_UNSPEC).
    Endpoint() noexcept = default;

    /// @brief Parses a dotted-quad IPv4 address.
    /// @throws std::invalid_argument if `address` is not a valid IPv4 address.
    [[nodiscard]] static Endpoint ipv4(std::string_view address, std::uint16_t port);

    /// @brief Parses a textual IPv6 address.
    /// @throws std::invalid_argument if `address` is not a valid IPv6 address.
    [[nodiscard]] static Endpoint ipv6(std::string_view address, std::uint16_t port);

    /// @brief 127.0.0.1 with the given port.
    [[nodiscard]] static Endpoint loopback(std::uint16_t port);

    /// @brief 0.0.0.0 (all IPv4 interfaces) with the given port.
    [[nodiscard]] static Endpoint any(std::uint16_t port);

    /// @brief Copies an address the kernel filled in (for example from `getsockname`).
    /// @throws std::invalid_argument if the family is not IPv4/IPv6 or `length` is too small.
    [[nodiscard]] static Endpoint fromSockaddr(const sockaddr* address, socklen_t length);

    /// @brief AF_INET, AF_INET6, or AF_UNSPEC when empty.
    [[nodiscard]] int family() const noexcept {
        return storage_.ss_family;
    }

    /// @brief The port in host byte order, or 0 when empty.
    [[nodiscard]] std::uint16_t port() const noexcept;

    /// @brief The address in numeric text form, or "" when empty.
    [[nodiscard]] std::string address() const;

    /// @brief `ip:port`, or `[ip]:port` for IPv6.
    [[nodiscard]] std::string toString() const;

    /// @brief The address to pass to socket calls.
    [[nodiscard]] const sockaddr* data() const noexcept {
        return reinterpret_cast<const sockaddr*>(&storage_);
    }

    /// @brief Size of the address behind `data()`.
    [[nodiscard]] socklen_t size() const noexcept {
        return length_;
    }

  private:
    sockaddr_storage storage_{}; ///< The address.
    socklen_t length_ = 0;       ///< Meaningful bytes of `storage_`.
};

} // namespace EventLoop
