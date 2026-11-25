# ============================================================
# Build configuration for the test suites.
# ============================================================

# Custom framework suite — see custom/README.md.
add_subdirectory(custom)

# Deliberate-misuse programs, expected to be caught by AddressSanitizer.
if(BUILD_MISUSE_TESTS)
    add_subdirectory(misuse)
endif()
