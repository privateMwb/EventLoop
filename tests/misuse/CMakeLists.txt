# ============================================================
# Deliberate-misuse programs. Each performs one documented lifetime
# mistake (docs/Lifetimes.md) and must be caught by AddressSanitizer.
# Enabled with -DBUILD_MISUSE_TESTS=ON.
# ============================================================

set(MISUSE_CASES staleResume danglingView destroyedClosure)

foreach(case ${MISUSE_CASES})
    add_executable(misuse_${case} ${case}.cpp)

    # Instrumented and unoptimised regardless of the build type, so the frame
    # allocations are real and the bug cannot be optimised away.
    target_compile_options(misuse_${case} PRIVATE -fsanitize=address -fno-omit-frame-pointer -O0 -g)
    target_link_options(misuse_${case} PRIVATE -fsanitize=address)

    target_include_directories(misuse_${case} PRIVATE ${PROJECT_SOURCE_DIR}/tests)
    target_link_libraries(misuse_${case} PRIVATE ${PROJECT_NAME})

    add_test(NAME Misuse.${case} COMMAND misuse_${case})
    set_tests_properties(Misuse.${case} PROPERTIES
        PASS_REGULAR_EXPRESSION "AddressSanitizer: heap-use-after-free")
endforeach()
