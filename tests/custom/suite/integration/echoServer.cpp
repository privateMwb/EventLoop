// EchoExample::EchoServer (callback version) integration suite.
//
// Coverage: the shared echo scenarios in <common/echoHarness.h>, run against
// the Phase 1 callback-only server.

#include <support/framework.h>

// clang-format off
#include <callback_echo/EchoServer.h> // EchoServer — the server under test
#include <common/echoHarness.h>       // echo::* — scenarios shared with the coroutine server
// clang-format on

using Server = EchoExample::EchoServer;

// Verifies a short message is echoed back unchanged.
static void echo_round_trips_message() {
    echo::roundTripsMessage<Server>();
}

// Verifies a payload far larger than the socket buffers survives backpressure.
static void echo_handles_large_payload() {
    echo::handlesLargePayload<Server>();
}

// Verifies many clients connect and are each echoed correctly.
static void echo_serves_many_clients() {
    echo::servesManyClients<Server>();
}

// Verifies queued echo data is flushed before a half-closed client is dropped.
static void echo_flushes_before_close() {
    echo::flushesBeforeClose<Server>();
}

// Verifies a disconnecting client is removed from the server.
static void echo_drops_closed_client() {
    echo::dropsClosedClient<Server>();
}

// Verifies destroying the server closes its clients' connections.
static void echo_closes_clients_on_destroy() {
    echo::closesClientsOnDestroy<Server>();
}

static void run_tests() {
    RUN(echo_round_trips_message);
    RUN(echo_handles_large_payload);
    RUN(echo_serves_many_clients);
    RUN(echo_flushes_before_close);
    RUN(echo_drops_closed_client);
    RUN(echo_closes_clients_on_destroy);
}

REGISTER_TEST_SUITE();
