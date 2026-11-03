# Warning policy applied to project-owned targets only. Third-party code is
# pulled in as SYSTEM so its headers do not trigger these diagnostics.

function(cc_set_warnings target)
    if(MSVC)
        set(warnings /W4 /permissive- /w14640 /w14265)
        if(CC_WARNINGS_AS_ERRORS)
            list(APPEND warnings /WX)
        endif()
    else()
        set(warnings
            -Wall
            -Wextra
            -Wpedantic
            -Wshadow
            -Wnon-virtual-dtor
            -Wold-style-cast
            -Wcast-align
            -Woverloaded-virtual
            -Wconversion
            -Wimplicit-fallthrough
            -Wformat=2)
        if(CC_WARNINGS_AS_ERRORS)
            list(APPEND warnings -Werror)
        endif()
    endif()
    target_compile_options(${target} PRIVATE ${warnings})
endfunction()
