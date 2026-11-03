# Global sanitizer instrumentation. Applied to every target (including
# third-party code) so that instrumented and uninstrumented code never mix,
# which matters especially for ThreadSanitizer.
#
#   -DCC_SANITIZER="address;undefined"   ASan + UBSan
#   -DCC_SANITIZER=thread                TSan (Linux/macOS only)

if(CC_SANITIZER)
    if(MSVC)
        if(CC_SANITIZER STREQUAL "address")
            add_compile_options(/fsanitize=address)
        else()
            message(FATAL_ERROR "MSVC only supports CC_SANITIZER=address")
        endif()
    else()
        list(JOIN CC_SANITIZER "," _cc_san)
        if(_cc_san MATCHES "thread" AND WIN32)
            message(FATAL_ERROR "ThreadSanitizer is not supported on Windows targets")
        endif()
        message(STATUS "ConcurrentCore: enabling -fsanitize=${_cc_san}")
        add_compile_options(-fsanitize=${_cc_san} -fno-omit-frame-pointer -g)
        add_link_options(-fsanitize=${_cc_san})
        if(_cc_san MATCHES "undefined")
            # Make UBSan findings fatal so tests fail loudly instead of
            # printing a warning that is easy to miss.
            add_compile_options(-fno-sanitize-recover=undefined)
        endif()
    endif()
endif()
