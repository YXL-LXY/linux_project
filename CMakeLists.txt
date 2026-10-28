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
# 默认关闭，保证没有安装 ROS2 的机器仍能构建 broker。
option(MQ_BUILD_ROS2_BRIDGE "Build mq_bridge with ROS2/rclcpp support" OFF)

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

# bridge 自己的协议和配置消息，不进入 broker 的协议依赖。
protobuf_generate_cpp(BRIDGE_PROTO_SRCS BRIDGE_PROTO_HDRS
    proto/bridge_config.proto
    proto/bridge_envelope.proto)

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
# mq_bridge_core: config, envelope, QoS mapping, retained store, adapters
# ---------------------------------------------------------------------------
# 这个库始终构建，因此 MQ 侧框架和单元测试不依赖 ROS2。
# 只有打开 MQ_BUILD_ROS2_BRIDGE 时才会编译 rclcpp 适配器。
add_library(mq_bridge_core STATIC
    ${BRIDGE_PROTO_SRCS}
    src/bridge/bridge_config.cc
    src/bridge/bridge_runtime.cc
    src/bridge/envelope_codec.cc
    src/bridge/mq_client_adapter.cc
    src/bridge/qos_mapper.cc
    src/bridge/retained_store.cc
    src/bridge/ros2_bridge.cc
)

target_include_directories(mq_bridge_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(mq_bridge_core PUBLIC mq_core)

if(MQ_BUILD_ROS2_BRIDGE)
    # ROS2 Humble 的 rclcpp 需要 C++14/17；只让 bridge 目标使用 17，
    # broker 本身仍保持 CMakeLists 顶部的 C++11。
    find_package(rclcpp REQUIRED)

    target_compile_features(mq_bridge_core PUBLIC cxx_std_17)
    target_compile_definitions(mq_bridge_core PUBLIC MQ_WITH_ROS2=1)
    target_link_libraries(mq_bridge_core PUBLIC rclcpp::rclcpp)
endif()

# ---------------------------------------------------------------------------
# Executables
# ---------------------------------------------------------------------------
add_executable(mqserver src/server/mq_server.cc)
target_link_libraries(mqserver PRIVATE mq_core)

add_executable(mq_bridge src/bridge/main.cc)
target_link_libraries(mq_bridge PRIVATE mq_bridge_core)

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
        mq_dispatchertest
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

    add_executable(mq_bridge_coretest tests/mq_bridge_coretest.cpp)
    target_link_libraries(mq_bridge_coretest PRIVATE mq_bridge_core GTest::gtest)
    add_test(NAME mq_bridge_coretest
        COMMAND ${CMAKE_COMMAND}
            -DTEST_EXE=$<TARGET_FILE:mq_bridge_coretest>
            -DDATA_DIR=${CMAKE_CURRENT_SOURCE_DIR}/tests/data
            -DWORK_DIR=${CMAKE_CURRENT_BINARY_DIR}/test-run/mq_bridge_coretest
            -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/RunTest.cmake)

    # 示例配置必须始终能通过 CLI 校验；这不依赖 ROS2 是否启用。
    add_test(NAME mq_bridge_config_check
        COMMAND $<TARGET_FILE:mq_bridge>
            --config ${CMAKE_CURRENT_SOURCE_DIR}/config/bridge.conf.example
            --check-config)
endif()
