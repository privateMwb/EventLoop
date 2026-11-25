/**
 * @file Version.cpp
 * @brief Library version string.
 */

// ============================================================
// Sections:
//   1. Version
// ============================================================

#include <EventLoop/Version.h> // EventLoop::version — the function this file implements

namespace EventLoop {

// ============================================================
// Section 1 — Version
// ============================================================

std::string_view version() noexcept {
    return "0.1.0";
}

} // namespace EventLoop
