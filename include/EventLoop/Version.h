/**
 * @file            Version.h
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
#include <string_view> // std::string_view — return type of version()
// clang-format on

namespace EventLoop {

/// @brief Library version as "major.minor.patch".
[[nodiscard]] std::string_view version() noexcept;

} // namespace EventLoop
