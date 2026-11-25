# ============================================================
# CMakeLists.txt
# Build configuration for the EventLoop library.
# ============================================================
#
# Sections:
#   1. C++ Configuration
#   2. Internal Submodule Libraries
#   3. EventLoop Library
#   4. Subdirectories
#   5. Installation
#   6. Packaging
#
# Retargeting after the name is picked: change only the name passed to
# project() below, and rename the src/<name> and include/<name>
# directories plus cmake/<name>Config.cmake.in to match.
# ============================================================

cmake_minimum_required(VERSION 3.20)

project(
    EventLoop
    VERSION 0.1.0
    LANGUAGES CXX
)

# ============================================================================
# C++ Configuration
# ============================================================================

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Place all executables directly in the build directory.
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

# The reactor is epoll-based. kqueue / IOCP are an open question, so fail
# loudly here instead of with an obscure <sys/epoll.h> error later.
# Android (Termux) reports CMAKE_SYSTEM_NAME as "Android" but uses the Linux
# kernel, so epoll is available there too.
if(NOT CMAKE_SYSTEM_NAME MATCHES "^(Linux|Android)$")
    message(FATAL_ERROR "EventLoop currently supports Linux/Android only (epoll reactor).")
endif()

# Coroutine support is where GCC/Clang versions diverge most. Minimums are
# enforced here and mirrored in the CI matrix.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 12)
    message(FATAL_ERROR "EventLoop requires GCC >= 12 (found ${CMAKE_CXX_COMPILER_VERSION}).")
endif()
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 16)
    message(FATAL_ERROR "EventLoop requires Clang >= 16 (found ${CMAKE_CXX_COMPILER_VERSION}).")
endif()

# ============================================================================
# Internal Submodule Libraries
# ============================================================================
#
# libs/internal/ -> libraries owned/maintained by this project
# (ThreadPoolPro, FunctionPro, PoolAllocator).
#
# Same wiring as FalconHTTP: glob each repo's include/ dir and compile its
# src/ directly into this library, rather than add_subdirectory() (target
# names don't always match folder names, e.g. PoolAllocator/ vs PoolPro).
# FalconHTTP is deliberately NOT a submodule here: it depends on EventLoop,
# not the other way round.

file(GLOB INTERNAL_LIB_INCLUDE_DIRS CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/libs/internal/*/include"
)

foreach(dir ${INTERNAL_LIB_INCLUDE_DIRS})
    message(STATUS "Include: ${dir}")
endforeach()

set(SUBMODULE_SOURCES "")
foreach(include_dir ${INTERNAL_LIB_INCLUDE_DIRS})
    get_filename_component(repo_root "${include_dir}" DIRECTORY)
    file(GLOB_RECURSE repo_sources CONFIGURE_DEPENDS "${repo_root}/src/*.cpp")
    if(repo_sources)
        message(STATUS "Compiling submodule sources from: ${repo_root}/src")
        list(APPEND SUBMODULE_SOURCES ${repo_sources})
    endif()
endforeach()

# ============================================================================
# EventLoop Library
# ============================================================================

file(GLOB_RECURSE LIB_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/src/${PROJECT_NAME}/*.cpp"
)
list(APPEND LIB_SOURCES ${SUBMODULE_SOURCES})

add_library(${PROJECT_NAME} STATIC ${LIB_SOURCES})

target_include_directories(
    ${PROJECT_NAME}
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# One BUILD_INTERFACE entry per submodule: a list inside a single generator
# expression would be split on ';' and break.
foreach(dir ${INTERNAL_LIB_INCLUDE_DIRS})
    target_include_directories(${PROJECT_NAME} PUBLIC $<BUILD_INTERFACE:${dir}>)
endforeach()

target_compile_features(${PROJECT_NAME} PUBLIC cxx_std_23)

find_package(Threads REQUIRED)
target_link_libraries(${PROJECT_NAME} PUBLIC Threads::Threads)

# ============================================================================
# Subdirectories
# ============================================================================

option(BUILD_TESTS "Build the test suite" ON)
option(BUILD_EXAMPLES "Build the examples" ON)
option(BUILD_BENCHMARKS "Build the benchmark suite" OFF)
option(BUILD_MISUSE_TESTS "Build the ASan lifetime-misuse programs (needs AddressSanitizer)" OFF)

enable_testing()

if(BUILD_TESTS)
    add_subdirectory(tests tests_build)
endif()

if(BUILD_EXAMPLES)
    add_subdirectory(examples examples_build)
endif()

if(BUILD_BENCHMARKS)
    add_subdirectory(benchmarks benchmarks_build)
endif()

# ============================================================================
# Installation
# ============================================================================

install(
    TARGETS ${PROJECT_NAME}
    EXPORT ${PROJECT_NAME}Targets
    ARCHIVE DESTINATION lib
)

install(
    EXPORT ${PROJECT_NAME}Targets
    FILE ${PROJECT_NAME}Targets.cmake
    NAMESPACE ${PROJECT_NAME}::
    DESTINATION lib/cmake/${PROJECT_NAME}
)

install(DIRECTORY include/ DESTINATION include)

# ============================================================================
# Packaging
# ============================================================================

include(CMakePackageConfigHelpers)

configure_package_config_file(
    cmake/${PROJECT_NAME}Config.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
    INSTALL_DESTINATION lib/cmake/${PROJECT_NAME}
)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

install(
    FILES
        ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
    DESTINATION lib/cmake/${PROJECT_NAME}
)
