include(GoogleTest)

function(cc_add_test_suite name label)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name} PRIVATE concurrentcore GTest::gtest_main)
    target_include_directories(${name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    cc_set_warnings(${name})
    # PRE_TEST discovery: the test binary is only executed by ctest, never
    # during the build (matters for sanitizer builds and cross-compiles).
    gtest_discover_tests(${name}
        DISCOVERY_MODE PRE_TEST
        PROPERTIES LABELS ${label} TIMEOUT 300)
endfunction()

cc_add_test_suite(cc_unit_tests unit
    unit/bounded_queue_test.cpp
    unit/thread_pool_test.cpp
    unit/job_test.cpp
    unit/job_manager_test.cpp
    unit/tasks_test.cpp
    unit/workspace_test.cpp
    unit/frame_test.cpp
    unit/request_handler_test.cpp)

cc_add_test_suite(cc_integration_tests integration
    integration/server_integration_test.cpp)

cc_add_test_suite(cc_stress_tests stress
    stress/stress_test.cpp)
