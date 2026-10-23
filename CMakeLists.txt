cmake_minimum_required(VERSION 3.16)

project(mq
    VERSION 1.0.0
    DESCRIPTION "A small RabbitMQ-like message broker built with muduo and protobuf"
    LANGUAGES CXX)

# ---------------------------------------------------------------------------
# Global settings
# ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

option(MQ_BUILD_TESTS "Build the unit tests" ON)
option(MQ_ENABLE_ASAN "Build with AddressSanitizer" OFF)

# ---------------------------------------------------------------------------
# Third-party dependencies
# ---------------------------------------------------------------------------
find_package(Protobuf REQUIRED)
find_package(SQLite3 REQUIRED)
find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)

set(MUDUO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party/muduo)
foreach(muduo_lib muduo_net muduo_base)
    if(NOT EXISTS ${MUDUO_DIR}/lib/lib${muduo_lib}.a)
        message(FATAL_ERROR
            "Missing ${MUDUO_DIR}/lib/lib${muduo_lib}.a. "
            "The vendored muduo libraries are required.")
    endif()
endforeach()

# ---------------------------------------------------------------------------
# Protocol Buffers
# ---------------------------------------------------------------------------
# Generated files go to ${CMAKE_BINARY_DIR}/mq_msg.pb.{h,cc} etc.
protobuf_generate_cpp(MQ_PROTO_SRCS MQ_PROTO_HDRS
    proto/mq_msg.proto
    proto/mq_proto.proto)

# ---------------------------------------------------------------------------
# mq_core: protobuf messages + muduo protobuf codec + common headers
# ---------------------------------------------------------------------------
add_library(mq_core STATIC
    ${MQ_PROTO_SRCS}
    third_party/muduo/include/muduo/proto/codec.cc
)

target_include_directories(mq_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_BINARY_DIR}
    ${MUDUO_DIR}/include
    ${Protobuf_INCLUDE_DIRS}
)

target_link_libraries(mq_core PUBLIC
    ${Protobuf_LIBRARIES}
    SQLite::SQLite3
    ZLIB::ZLIB
    Threads::Threads
    ${MUDUO_DIR}/lib/libmuduo_net.a
    ${MUDUO_DIR}/lib/libmuduo_base.a
)

if(MQ_ENABLE_ASAN)
    target_compile_options(mq_core PUBLIC -fsanitize=address -fno-omit-frame-pointer -g)
    target_link_options(mq_core PUBLIC -fsanitize=address)
endif()

# ---------------------------------------------------------------------------
# Executables
# ---------------------------------------------------------------------------
add_executable(mqserver src/server/mq_server.cc)
target_link_libraries(mqserver PRIVATE mq_core)

add_executable(publish_client src/client/publish_client.cc)
target_link_libraries(publish_client PRIVATE mq_core)

add_executable(consume_client src/client/consume_client.cc)
target_link_libraries(consume_client PRIVATE mq_core)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
if(MQ_BUILD_TESTS)
    enable_testing()
    find_package(GTest REQUIRED)

    set(MQ_TESTS
        mq_bindingtest
        mq_channeltest
        mq_connectiontest
        mq_consumertest
        mq_exchangetest
        mq_filetest
        mq_hosttest
        mq_msgtest
        mq_queuetest
        mq_routetest)

    foreach(test_name IN LISTS MQ_TESTS)
        add_executable(${test_name} tests/${test_name}.cpp)
        target_link_libraries(${test_name} PRIVATE mq_core GTest::gtest)

        # Each test runs in its own directory with a fresh copy of tests/data.
        # This keeps the SQLite fixtures independent between tests and runs.
        add_test(NAME ${test_name}
            COMMAND ${CMAKE_COMMAND}
                -DTEST_EXE=$<TARGET_FILE:${test_name}>
                -DDATA_DIR=${CMAKE_CURRENT_SOURCE_DIR}/tests/data
                -DWORK_DIR=${CMAKE_CURRENT_BINARY_DIR}/test-run/${test_name}
                -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/RunTest.cmake)
    endforeach()
endif()
