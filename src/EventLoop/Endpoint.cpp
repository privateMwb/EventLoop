/**
 * @file Endpoint.cpp
 * @brief Endpoint parsing and formatting.
 */

// ============================================================
// Implementation for EventLoop::Endpoint.
// ============================================================
//
//  Sections:
//   1. Construction
//   2. Accessors
//
// ============================================================

// clang-format off
#include <EventLoop/Endpoint.h> // Endpoint — the class this file implements

#include <arpa/inet.h>          // inet_pton, inet_ntop, htons, ntohs — text and byte-order conversion
#include <cstring>              // std::memcpy — copying a kernel-provided address
#include <stdexcept>            // std::invalid_argument — rejecting bad input
// clang-format on

namespace EventLoop {

// ============================================================
// Section 1 — Construction
// ============================================================

Endpoint Endpoint::ipv4(std::string_view address, std::uint16_t port) {
    Endpoint endpoint;
    sockaddr_in raw{};
    raw.sin_family = AF_INET;
    raw.sin_port = htons(port);
    const std::string text(address); // inet_pton needs a terminated string.
    if (::inet_pton(AF_INET, text.c_str(), &raw.sin_addr) != 1) {
        throw std::invalid_argument("Endpoint::ipv4: invalid address '" + text + "'");
    }
    std::memcpy(&endpoint.storage_, &raw, sizeof(raw));
    endpoint.length_ = sizeof(raw);
    return endpoint;
}

Endpoint Endpoint::ipv6(std::string_view address, std::uint16_t port) {
    Endpoint endpoint;
    sockaddr_in6 raw{};
    raw.sin6_family = AF_INET6;
    raw.sin6_port = htons(port);
    const std::string text(address);
    if (::inet_pton(AF_INET6, text.c_str(), &raw.sin6_addr) != 1) {
        throw std::invalid_argument("Endpoint::ipv6: invalid address '" + text + "'");
    }
    std::memcpy(&endpoint.storage_, &raw, sizeof(raw));
    endpoint.length_ = sizeof(raw);
    return endpoint;
}

Endpoint Endpoint::loopback(std::uint16_t port) {
    return ipv4("127.0.0.1", port);
}

Endpoint Endpoint::any(std::uint16_t port) {
    return ipv4("0.0.0.0", port);
}

Endpoint Endpoint::fromSockaddr(const sockaddr* address, socklen_t length) {
    Endpoint endpoint;
    if (address == nullptr || length < sizeof(sa_family_t)) {
        throw std::invalid_argument("Endpoint::fromSockaddr: address too short");
    }
    const std::size_t needed = address->sa_family == AF_INET    ? sizeof(sockaddr_in)
                               : address->sa_family == AF_INET6 ? sizeof(sockaddr_in6)
                                                                : 0;
    if (needed == 0 || length < needed) {
        throw std::invalid_argument("Endpoint::fromSockaddr: unsupported or truncated address");
    }
    std::memcpy(&endpoint.storage_, address, needed);
    endpoint.length_ = static_cast<socklen_t>(needed);
    return endpoint;
}

// ============================================================
// Section 2 — Accessors
// ============================================================

std::uint16_t Endpoint::port() const noexcept {
    if (family() == AF_INET) {
        return ntohs(reinterpret_cast<const sockaddr_in*>(&storage_)->sin_port);
    }
    if (family() == AF_INET6) {
        return ntohs(reinterpret_cast<const sockaddr_in6*>(&storage_)->sin6_port);
    }
    return 0;
}

std::string Endpoint::address() const {
    char buffer[INET6_ADDRSTRLEN] = {};
    if (family() == AF_INET) {
        ::inet_ntop(AF_INET, &reinterpret_cast<const sockaddr_in*>(&storage_)->sin_addr, buffer,
                    sizeof(buffer));
    } else if (family() == AF_INET6) {
        ::inet_ntop(AF_INET6, &reinterpret_cast<const sockaddr_in6*>(&storage_)->sin6_addr, buffer,
                    sizeof(buffer));
    }
    return buffer;
}

std::string Endpoint::toString() const {
    if (family() == AF_INET6) {
        return "[" + address() + "]:" + std::to_string(port());
    }
    return address() + ":" + std::to_string(port());
}

} // namespace EventLoop
