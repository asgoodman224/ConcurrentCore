# Reproducible third-party dependencies. Every archive is pinned to an exact
# release and verified with a SHA-256 hash. All dependencies are declared
# SYSTEM (warnings in their headers are suppressed) and EXCLUDE_FROM_ALL
# (only the targets we actually link are built).

include(FetchContent)
set(FETCHCONTENT_QUIET ON)

# --- Standalone Asio (header-only) ------------------------------------------
FetchContent_Declare(asio
    URL https://github.com/chriskohlhoff/asio/archive/refs/tags/asio-1-36-0.tar.gz
    URL_HASH SHA256=0310a76b27e1854f09f696b30de57dc490b5e1b17faed1eb8c9a2891f956e52b
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    SYSTEM EXCLUDE_FROM_ALL)
FetchContent_MakeAvailable(asio)

add_library(cc_asio INTERFACE)
add_library(concurrentcore::asio ALIAS cc_asio)
target_include_directories(cc_asio SYSTEM INTERFACE ${asio_SOURCE_DIR}/asio/include)
target_compile_definitions(cc_asio INTERFACE ASIO_STANDALONE ASIO_NO_DEPRECATED)
target_link_libraries(cc_asio INTERFACE Threads::Threads)
if(WIN32)
    # Target Windows 10+ and link the Winsock libraries Asio needs.
    target_compile_definitions(cc_asio INTERFACE _WIN32_WINNT=0x0A00 WIN32_LEAN_AND_MEAN NOMINMAX)
    target_link_libraries(cc_asio INTERFACE ws2_32 mswsock)
endif()
if(MINGW)
    # ODR guard (found while debugging a shutdown hang, see
    # docs/DESIGN_DECISIONS.md): Asio defines ASIO_HAS_PTHREADS only if
    # _POSIX_THREADS is already defined when its config header is first
    # included. On MinGW, winpthreads defines _POSIX_THREADS, and libstdc++'s
    # <thread>/<mutex> include it. Translation units that include <thread>
    # before <asio.hpp> therefore get asio::detail::thread = posix_thread,
    # while the others get win_thread: two incompatible layouts of the same
    # inline classes in one program. Pinning the choice makes every TU agree
    # regardless of include order.
    target_compile_definitions(cc_asio INTERFACE ASIO_HAS_PTHREADS=1)
endif()

# --- nlohmann/json (header-only) --------------------------------------------
FetchContent_Declare(nlohmann_json
    URL https://github.com/nlohmann/json/releases/download/v3.12.0/json.tar.xz
    URL_HASH SHA256=42f6e95cad6ec532fd372391373363b62a14af6d771056dbfc86160e6dfff7aa
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    SYSTEM EXCLUDE_FROM_ALL)
FetchContent_MakeAvailable(nlohmann_json)

# --- spdlog (compiled, bundled fmt) -----------------------------------------
set(SPDLOG_BUILD_EXAMPLE OFF CACHE BOOL "" FORCE)
set(SPDLOG_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(SPDLOG_INSTALL OFF CACHE BOOL "" FORCE)
FetchContent_Declare(spdlog
    URL https://github.com/gabime/spdlog/archive/refs/tags/v1.16.0.tar.gz
    URL_HASH SHA256=8741753e488a78dd0d0024c980e1fb5b5c85888447e309d9cb9d949bdb52aa3e
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    SYSTEM EXCLUDE_FROM_ALL)
FetchContent_MakeAvailable(spdlog)

# --- zlib (compression / decompression) -------------------------------------
set(ZLIB_BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(ZLIB_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(ZLIB_BUILD_STATIC ON CACHE BOOL "" FORCE)
set(ZLIB_INSTALL OFF CACHE BOOL "" FORCE)
FetchContent_Declare(zlib
    URL https://github.com/madler/zlib/releases/download/v1.3.2/zlib-1.3.2.tar.gz
    URL_HASH SHA256=bb329a0a2cd0274d05519d61c667c062e06990d72e125ee2dfa8de64f0119d16
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    SYSTEM EXCLUDE_FROM_ALL)
FetchContent_MakeAvailable(zlib)

# --- Mbed TLS 3.6 LTS (SHA-256 implementation only: mbedcrypto) -------------
set(ENABLE_PROGRAMS OFF CACHE BOOL "" FORCE)
set(ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(MBEDTLS_FATAL_WARNINGS OFF CACHE BOOL "" FORCE)
set(GEN_FILES OFF CACHE BOOL "" FORCE)
set(USE_SHARED_MBEDTLS_LIBRARY OFF CACHE BOOL "" FORCE)
set(USE_STATIC_MBEDTLS_LIBRARY ON CACHE BOOL "" FORCE)
FetchContent_Declare(mbedtls
    URL https://github.com/Mbed-TLS/mbedtls/releases/download/mbedtls-3.6.5/mbedtls-3.6.5.tar.bz2
    URL_HASH SHA256=4a11f1777bb95bf4ad96721cac945a26e04bf19f57d905f241fe77ebeddf46d8
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    SYSTEM EXCLUDE_FROM_ALL)
FetchContent_MakeAvailable(mbedtls)

# --- GoogleTest --------------------------------------------------------------
if(CC_BUILD_TESTS)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_Declare(googletest
        URL https://github.com/google/googletest/releases/download/v1.17.0/googletest-1.17.0.tar.gz
        URL_HASH SHA256=65fab701d9829d38cb77c14acdc431d2108bfdbf8979e40eb8ae567edf10b27c
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
        SYSTEM EXCLUDE_FROM_ALL)
    FetchContent_MakeAvailable(googletest)
endif()
