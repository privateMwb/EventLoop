// EventLoop::Endpoint test suite.
//
// Coverage:
// - IPv4 and IPv6 parsing, port and text round trips
// - Rejection of malformed addresses
// - Conversion from a kernel-provided sockaddr

#include <support/framework.h>

// clang-format off
#include <stdexcept> // std::invalid_argument — expected from bad input
// clang-format on

using namespace EventLoop;

// Verifies an IPv4 endpoint reports its parts and formats as ip:port.
static void ipv4_round_trips_fields() {
    const Endpoint endpoint = Endpoint::ipv4("192.168.1.20", 8080);

    CHK(endpoint.family() == AF_INET);
    CHK(endpoint.port() == 8080);
    CHK(endpoint.address() == "192.168.1.20");
    CHK(endpoint.toString() == "192.168.1.20:8080");
    CHK(endpoint.size() == sizeof(sockaddr_in));
}

// Verifies an IPv6 endpoint reports its parts and brackets the address.
static void ipv6_round_trips_fields() {
    const Endpoint endpoint = Endpoint::ipv6("::1", 9000);

    CHK(endpoint.family() == AF_INET6);
    CHK(endpoint.port() == 9000);
    CHK(endpoint.address() == "::1");
    CHK(endpoint.toString() == "[::1]:9000");
}

// Verifies the loopback and any-address shortcuts.
static void shortcuts_build_expected_addresses() {
    CHK(Endpoint::loopback(1).toString() == "127.0.0.1:1");
    CHK(Endpoint::any(2).toString() == "0.0.0.0:2");
}

// Verifies malformed addresses are rejected.
static void bad_addresses_are_rejected() {
    CHK_THROWS(Endpoint::ipv4("not-an-ip", 1), std::invalid_argument);
    CHK_THROWS(Endpoint::ipv4("256.0.0.1", 1), std::invalid_argument);
    CHK_THROWS(Endpoint::ipv6("127.0.0.1", 1), std::invalid_argument);
    CHK_THROWS(Endpoint::ipv6("", 1), std::invalid_argument);
}

// Verifies an empty endpoint is unspecified.
static void empty_endpoint_is_unspecified() {
    const Endpoint endpoint;

    CHK(endpoint.family() == AF_UNSPEC);
    CHK(endpoint.port() == 0);
    CHK(endpoint.address().empty());
}

// Verifies a copied kernel address matches the original.
static void from_sockaddr_copies_address() {
    const Endpoint original = Endpoint::ipv4("10.1.2.3", 4242);

    const Endpoint copy = Endpoint::fromSockaddr(original.data(), original.size());

    CHK(copy.toString() == original.toString());
    CHK_THROWS(Endpoint::fromSockaddr(original.data(), 1), std::invalid_argument);
}

static void run_tests() {
    RUN(ipv4_round_trips_fields);
    RUN(ipv6_round_trips_fields);
    RUN(shortcuts_build_expected_addresses);
    RUN(bad_addresses_are_rejected);
    RUN(empty_endpoint_is_unspecified);
    RUN(from_sockaddr_copies_address);
}

REGISTER_TEST_SUITE();
