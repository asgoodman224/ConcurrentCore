add_executable(concurrentcore-bench bench_main.cpp)
target_link_libraries(concurrentcore-bench PRIVATE concurrentcore)
target_compile_definitions(concurrentcore-bench PRIVATE
    CC_BENCH_BUILD_TYPE="${CMAKE_BUILD_TYPE}"
    CC_BENCH_SYSTEM="${CMAKE_SYSTEM_NAME} ${CMAKE_SYSTEM_VERSION}")
cc_set_warnings(concurrentcore-bench)
