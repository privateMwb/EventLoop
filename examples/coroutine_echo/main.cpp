/**
 * @file main.cpp
 * @brief Runs the coroutine echo server until interrupted.
 */

// clang-format off
#include <coroutine_echo/EchoServer.h> // EchoServer, Reactor — the server and the loop it runs on

#include <csignal>                     // std::signal, std::sig_atomic_t, SIGINT, SIGTERM — clean shutdown
#include <cstdint>                     // std::uint16_t — port argument
#include <iostream>                    // std::cout — startup message
#include <string>                      // std::stoi — port argument parsing
// clang-format on

namespace {

volatile std::sig_atomic_t gStop = 0; ///< Set by the signal handler, polled by the loop.

void onSignal(int) {
    gStop = 1;
}

} // namespace

int main(int argc, char* argv[]) {
    const auto port = static_cast<std::uint16_t>(argc > 1 ? std::stoi(argv[1]) : 7000);

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    EventLoop::Reactor reactor;
    CoroEchoExample::EchoServer server(reactor, port);
    std::cout << "coroutine echo server listening on 127.0.0.1:" << server.port() << std::endl;

    // A finite wait lets the loop notice the signal flag between events.
    while (gStop == 0) {
        reactor.runOnce(100);
    }
    return 0;
}
