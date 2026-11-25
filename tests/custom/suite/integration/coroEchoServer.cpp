// CoroEchoExample::EchoServer (coroutine version) integration suite.
//
// Coverage: the shared echo scenarios in <common/echoHarness.h>, run against
// the Phase 3 coroutine server. It must pass exactly what the callback server passes.

#include <support/framework.h>

// clang-format off
#include <coroutine_echo/EchoServer.h> // EchoServer — the server under test
#include <common/echoHarness.h>       // echo::* — scenarios shared with the callback server
// clang-format on

using Server = CoroEchoExample::EchoServer;

// Verifies a short message is echoed back unchanged.
static void coro_echo_round_trips_message() {
    echo::roundTripsMessage<Server>();
}

// Verifies a payload far larger than the socket buffers survives backpressure.
static void coro_echo_handles_large_payload() {
    echo::handlesLargePayload<Server>();
}

// Verifies many clients connect and are each echoed correctly.
static void coro_echo_serves_many_clients() {
    echo::servesManyClients<Server>();
}

// Verifies queued echo data is flushed before a half-closed client is dropped.
static void coro_echo_flushes_before_close() {
    echo::flushesBeforeClose<Server>();
}

// Verifies a disconnecting client is removed from the server.
static void coro_echo_drops_closed_client() {
    echo::dropsClosedClient<Server>();
}

// Verifies destroying the server closes its clients' connections.
static void coro_echo_closes_clients_on_destroy() {
    echo::closesClientsOnDestroy<Server>();
}

static void run_tests() {
    RUN(coro_echo_round_trips_message);
    RUN(coro_echo_handles_large_payload);
    RUN(coro_echo_serves_many_clients);
    RUN(coro_echo_flushes_before_close);
    RUN(coro_echo_drops_closed_client);
    RUN(coro_echo_closes_clients_on_destroy);
}

REGISTER_TEST_SUITE();
