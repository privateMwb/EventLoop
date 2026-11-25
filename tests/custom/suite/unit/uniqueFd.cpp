// EventLoop::UniqueFd and setNonBlocking test suite.
//
// Coverage:
// - Ownership and closing on destruction
// - Move, release and reset semantics
// - setNonBlocking success and failure

// clang-format off
#include <support/framework.h>

#include <cerrno>     // errno, EBADF — confirming a descriptor was closed
#include <fcntl.h>    // fcntl, F_GETFL, F_GETFD, O_NONBLOCK — inspecting descriptors
#include <system_error> // std::system_error — expected from setNonBlocking
// clang-format on

using namespace EventLoop;

/// True if `fd` no longer refers to an open descriptor.
static bool isClosed(int fd) {
    return ::fcntl(fd, F_GETFD) < 0 && errno == EBADF;
}

// Verifies a default-constructed wrapper holds nothing.
static void default_is_empty() {
    UniqueFd fd;
    CHK(!fd.valid());
    CHK(fd.get() == -1);
}

// Verifies the descriptor is closed when the wrapper dies.
static void destructor_closes_descriptor() {
    int raw = -1;
    {
        Pipe pipe = makePipe();
        raw = pipe.readEnd.get();
        CHK(!isClosed(raw));
    }
    CHK(isClosed(raw));
}

// Verifies moving transfers ownership without closing.
static void move_transfers_ownership() {
    Pipe pipe = makePipe();
    const int raw = pipe.readEnd.get();

    UniqueFd moved(std::move(pipe.readEnd));

    CHK(moved.get() == raw);
    CHK(!pipe.readEnd.valid());
    CHK(!isClosed(raw));
}

// Verifies release() hands back the descriptor still open.
static void release_keeps_descriptor_open() {
    Pipe pipe = makePipe();
    const int raw = pipe.readEnd.release();

    CHK(!pipe.readEnd.valid());
    CHK(!isClosed(raw));
    UniqueFd adopt(raw); // Re-own it so the test does not leak.
}

// Verifies reset() closes the old descriptor and adopts the new one.
static void reset_replaces_descriptor() {
    Pipe a = makePipe();
    Pipe b = makePipe();
    const int oldFd = a.readEnd.get();
    const int newFd = b.readEnd.release();

    a.readEnd.reset(newFd);

    CHK(isClosed(oldFd));
    CHK(a.readEnd.get() == newFd);
}

// Verifies setNonBlocking sets O_NONBLOCK on a descriptor.
static void set_nonblocking_sets_flag() {
    int fds[2];
    CHK(::pipe(fds) == 0);
    UniqueFd readEnd(fds[0]);
    UniqueFd writeEnd(fds[1]);
    CHK((::fcntl(readEnd.get(), F_GETFL) & O_NONBLOCK) == 0);

    setNonBlocking(readEnd.get());

    CHK((::fcntl(readEnd.get(), F_GETFL) & O_NONBLOCK) != 0);
}

// Verifies setNonBlocking reports failure for a bad descriptor.
static void set_nonblocking_rejects_bad_fd() {
    CHK_THROWS(setNonBlocking(-1), std::system_error);
}

static void run_tests() {
    RUN(default_is_empty);
    RUN(destructor_closes_descriptor);
    RUN(move_transfers_ownership);
    RUN(release_keeps_descriptor_open);
    RUN(reset_replaces_descriptor);
    RUN(set_nonblocking_sets_flag);
    RUN(set_nonblocking_rejects_bad_fd);
}

REGISTER_TEST_SUITE();
