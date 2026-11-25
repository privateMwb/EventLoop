#pragma once

// Small descriptor fixtures shared by the suites.

// clang-format off
#include <EventLoop/Fd.h> // UniqueFd — owns the pipe ends

#include <cerrno>         // errno — pipe2 failure
#include <fcntl.h>        // O_NONBLOCK, O_CLOEXEC — pipe flags
#include <system_error>   // std::system_error — pipe2 failure
#include <unistd.h>       // pipe2, read, write — pipe I/O
// clang-format on

/// Both ends of a non-blocking pipe.
struct Pipe {
    EventLoop::UniqueFd readEnd;
    EventLoop::UniqueFd writeEnd;
};

/// Creates a non-blocking pipe.
inline Pipe makePipe() {
    int fds[2];
    if (::pipe2(fds, O_NONBLOCK | O_CLOEXEC) != 0) {
        throw std::system_error(errno, std::generic_category(), "pipe2");
    }
    return Pipe{EventLoop::UniqueFd(fds[0]), EventLoop::UniqueFd(fds[1])};
}

/// Writes one byte, making the read end readable.
inline void writeByte(int fd) {
    const char byte = 'x';
    (void)!::write(fd, &byte, 1);
}

/// Reads and discards everything currently readable.
inline void drain(int fd) {
    char buffer[64];
    while (::read(fd, buffer, sizeof(buffer)) > 0) {
    }
}
