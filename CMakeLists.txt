cmake_minimum_required(VERSION 3.2)

set(PATCH_VERSION "2" CACHE INTERNAL "Patch version")
set(PROJECT_VERSION 0.0.${PATCH_VERSION})

project(mapreduce VERSION ${PROJECT_VERSION})

option(WITH_GTEST "Whether to build google test" ON)
option(GIT_SUBMODULE "Check submodules during build" ON)

configure_file(version.h.in version.h)

find_package(Threads REQUIRED)
find_package(Boost COMPONENTS system REQUIRED)
find_package(Boost COMPONENTS program_options REQUIRED)
if(NOT Boost_FOUND)
    print("Boost was not found")
endif()


add_library(framework_lib
    "mapreduce.hpp"
    "mapreduce.cpp"
)
set_target_properties(framework_lib PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)
target_include_directories(framework_lib PRIVATE
    #${SQLite3_INCLUDE_DIRS}
)
target_link_libraries(framework_lib PRIVATE
    ${CMAKE_THREAD_LIBS_INIT}
)

add_library(prefix_lib
    "prefix.hpp"
    "prefix.cpp"
)
set_target_properties(prefix_lib PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)
target_include_directories(prefix_lib PRIVATE
    ${CMAKE_BINARY_DIR}
)
target_link_libraries(prefix_lib PRIVATE
    framework_lib
    ${CMAKE_THREAD_LIBS_INIT}
)

add_library(wcount_lib
    "words_count.hpp"
    "words_count.cpp"
)
set_target_properties(wcount_lib PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)
target_include_directories(wcount_lib PRIVATE
    ${CMAKE_BINARY_DIR}
)
target_link_libraries(wcount_lib PRIVATE
    framework_lib
    ${CMAKE_THREAD_LIBS_INIT}
)

add_executable(${PROJECT_NAME}
    "main.cpp"
    "input.hpp"
    "input.cpp"
)
set_target_properties(${PROJECT_NAME} PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)
target_include_directories(${PROJECT_NAME} PRIVATE
    ${CMAKE_BINARY_DIR}
    ${Boost_INCLUDE_DIR}
)
target_link_libraries(${PROJECT_NAME} PRIVATE
    framework_lib
    prefix_lib
    wcount_lib
    ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES}
)

if(WITH_GTEST)
    find_package(GTest QUIET)
    if(GTest_FOUND)
        message(STATUS "GTest is found")
    else()
        find_package(Git REQUIRED)
        if(GIT_FOUND AND EXISTS "${PROJECT_SOURCE_DIR}/.git")
            # Update submodules as needed
            message(STATUS "Git is found")
            if(GIT_SUBMODULE)
                message(STATUS "Submodules: add")

                if(WITH_GTEST)
                    if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/googletest")
                        execute_process(COMMAND ${GIT_EXECUTABLE}
                                        submodule add https://github.com/google/googletest.git googletest
                                        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                                        RESULT_VARIABLE GIT_SUBMOD_RESULT)
                        if(NOT GIT_SUBMOD_RESULT EQUAL "0")
                            message(FATAL_ERROR "git submodule add https://github.com/google/googletest.git googletest failed
                                    with ${GIT_SUBMOD_RESULT}, please checkout submodules")
                        else()
                            message(STATUS "GTest submodule added")
                        endif()
                    endif()
                endif()

                message(STATUS "Submodules: update")
                execute_process(COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive
                                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                                RESULT_VARIABLE GIT_SUBMOD_RESULT)
                if(NOT GIT_SUBMOD_RESULT EQUAL "0")
                    message(FATAL_ERROR "git submodule update --init --recursive failed
                            with ${GIT_SUBMOD_RESULT}, please checkout submodules")
                else()
                    message(STATUS "Submodules updated")
                endif()
            endif()
        endif()
    endif()
endif()

if(WITH_GTEST)
    if(NOT GTest_FOUND)
        add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/googletest" "googletest")
        message(STATUS "Add googletest subdirectory")
    endif()
    add_executable(test_mapreduce "test.cpp")

    target_include_directories(test_mapreduce PRIVATE
        ${GTEST_INCLUDE_DIRS}
#        "${CMAKE_CURRENT_SOURCE_DIR}/src"
    )

    set_target_properties(test_mapreduce PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED ON
    )

#    target_compile_definitions(test_print_ip PRIVATE
#        $<$<CONFIG:Debug>:DEBUG_VER>
#        $<$<CONFIG:Release>:RELEASE_VER>
#    )

    target_link_libraries(test_mapreduce PRIVATE
        ${GTEST_BOTH_LIBRARIES}
        gtest
        gtest_main
        prefix_lib
        wcount_lib
        ${CMAKE_THREAD_LIBS_INIT}
    )
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    target_compile_options(${PROJECT_NAME} PRIVATE
        /W4 /await
    )
    if(WITH_GTEST)
        target_compile_options(test_mapreduce PRIVATE
            /W4 /await
        )
    endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options(${PROJECT_NAME} PRIVATE
        "-Wall" "-Wextra" "-Werror" "-pedantic" "-fcoroutines"
    )
    if(WITH_GTEST)
        target_compile_options(test_mapreduce PRIVATE
            "-Wall" "-Wextra" "-Werror" "-pedantic" "-fcoroutines"
        )
    endif()
endif()

install(TARGETS ${PROJECT_NAME} RUNTIME DESTINATION bin)

set(CPACK_GENERATOR DEB)
set(CPACK_PACKAGE_VERSION_MAJOR "${PROJECT_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${PROJECT_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${PROJECT_VERSION_PATCH}")
set(CPACK_PACKAGE_CONTACT divmy@yandex.ru)
include(CPack)

if(WITH_GTEST)
    include(CTest)
    enable_testing()
    list(APPEND CMAKE_CTEST_ARGUMENTS "--rerun-failed" "--output-on-failure")
    add_test(test test_mapreduce)
endif()
