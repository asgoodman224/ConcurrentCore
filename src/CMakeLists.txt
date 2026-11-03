# Core library: everything except the two program entry points. Tests and
# benchmarks link against it directly.
add_library(concurrentcore STATIC
    common/error.cpp
    concurrency/thread_pool.cpp
    jobs/job.cpp
    jobs/job_registry.cpp
    jobs/job_manager.cpp
    tasks/task_spec.cpp
    tasks/workspace.cpp
    tasks/file_io.cpp
    tasks/sha256_task.cpp
    tasks/compression_task.cpp
    tasks/text_analysis_task.cpp
    tasks/compute_task.cpp
    tasks/task_dispatch.cpp
    networking/frame.cpp
    networking/protocol.cpp
    server/server_config.cpp
    server/request_handler.cpp
    server/session.cpp
    server/tcp_server.cpp
    client/client.cpp
)
add_library(concurrentcore::concurrentcore ALIAS concurrentcore)

target_include_directories(concurrentcore PUBLIC ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(concurrentcore
    PUBLIC
        concurrentcore::asio
        nlohmann_json::nlohmann_json
        spdlog::spdlog
        Threads::Threads
    PRIVATE
        ZLIB::ZLIBSTATIC
        MbedTLS::mbedcrypto)
cc_set_warnings(concurrentcore)

add_executable(concurrentcore-server server/main.cpp)
target_link_libraries(concurrentcore-server PRIVATE concurrentcore)
cc_set_warnings(concurrentcore-server)

add_executable(concurrentcore-client client/main.cpp)
target_link_libraries(concurrentcore-client PRIVATE concurrentcore)
cc_set_warnings(concurrentcore-client)
