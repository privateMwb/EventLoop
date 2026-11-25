/**
 * @file            EventLoop.h
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
#include <EventLoop/Endpoint.h>  // Endpoint — IPv4/IPv6 address and port
#include <EventLoop/Event.h>   // Event — readiness flags
#include <EventLoop/Fd.h>      // UniqueFd, setNonBlocking — descriptor ownership
#include <EventLoop/Reactor.h> // Reactor — the epoll reactor
#include <EventLoop/Sleep.h>   // sleepFor, sleepUntil — awaitable delays
#include <EventLoop/Socket.h>  // Socket — awaitable stream socket
#include <EventLoop/Start.h>   // spawn, syncWait — ways to start a Task
#include <EventLoop/Task.h>    // Task — lazy coroutine task
#include <EventLoop/Timeout.h> // withTimeout, TimeoutError — time-limited tasks
#include <EventLoop/Version.h> // EventLoop::version — library version string
// clang-format on

/// @brief Umbrella so this library's types are reachable as `rain::...`,
/// alongside every other project library. Reopens `rain` rather than
/// aliasing it; this is the only place that happens.
namespace rain {
using namespace EventLoop;
}
