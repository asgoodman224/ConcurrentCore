cmake_minimum_required(VERSION 3.25)

project(ConcurrentCore
    VERSION 1.0.0
    DESCRIPTION "Multithreaded task processing server (C++20, Asio, producer-consumer thread pool)"
    LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

option(CC_BUILD_TESTS        "Build the GoogleTest suites"                 ON)
option(CC_BUILD_BENCHMARKS   "Build the benchmark harness"                 ON)
option(CC_BUILD_EXAMPLES     "Build the example programs"                  ON)
option(CC_WARNINGS_AS_ERRORS "Treat warnings as errors for project code"   ON)
set(CC_SANITIZER "" CACHE STRING "Sanitizers to enable: '', 'address;undefined', or 'thread'")

# Put every executable in one directory so runtime DLLs (Windows) are found
# next to the binaries and scripts have a predictable path.
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
include(CompilerWarnings)
include(Sanitizers)
include(Dependencies)

find_package(Threads REQUIRED)

if(MINGW AND NOT CC_SANITIZER)
    # Self-contained executables: no dependency on the toolchain's
    # libstdc++/libgcc/winpthread DLLs being on PATH.
    add_link_options(-static)
endif()

add_subdirectory(src)

if(CC_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

if(CC_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

if(CC_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()
