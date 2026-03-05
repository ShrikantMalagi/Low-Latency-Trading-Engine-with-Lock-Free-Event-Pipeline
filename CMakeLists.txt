cmake_minimum_required(VERSION 3.22)
cmake_policy(SET CMP0135 NEW)
project(hft_sim LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wall -Wextra -Wpedantic)

#Libraries
add_library(protocol INTERFACE)
target_include_directories(protocol INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)

#Exchange service
add_executable(exchange_sim
  services/exchange/main.cpp
  services/exchange/exchange.cpp
)
target_include_directories(exchange_sim PRIVATE include)
target_link_libraries(exchange_sim PRIVATE protocol)

add_executable(exchange_server
  services/exchange/server_main.cpp
  services/exchange/exchange.cpp
  services/coordination/order_coordinator.cpp
  services/oms/oms.cpp
)
target_include_directories(exchange_server PRIVATE include)
target_link_libraries(exchange_server PRIVATE protocol)

# OMS service
add_executable(oms_sim
  services/oms/oms_main.cpp
  services/oms/oms.cpp
)
target_include_directories(oms_sim PRIVATE include)
target_link_libraries(oms_sim PRIVATE protocol)

#Tests
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(exchange_tests
  tests/test_exchange.cpp
  services/exchange/exchange.cpp
)
target_include_directories(exchange_tests PRIVATE include)
target_link_libraries(exchange_tests PRIVATE GTest::gtest_main protocol)

add_executable(oms_tests
  tests/test_oms.cpp
  services/oms/oms.cpp
)
target_include_directories(oms_tests PRIVATE include)
target_link_libraries(oms_tests PRIVATE GTest::gtest_main protocol)

add_executable(order_coordinator_tests
  tests/test_order_coordinator.cpp
  services/coordination/order_coordinator.cpp
  services/oms/oms.cpp
  services/exchange/exchange.cpp
)
target_include_directories(order_coordinator_tests PRIVATE include)
target_link_libraries(order_coordinator_tests PRIVATE GTest::gtest_main protocol)

add_executable(server_integration_tests
  tests/test_server_integration.cpp
)
target_include_directories(server_integration_tests PRIVATE include)
target_compile_definitions(
  server_integration_tests PRIVATE
  EXCHANGE_SERVER_PATH="$<TARGET_FILE:exchange_server>"
)
target_link_libraries(server_integration_tests PRIVATE GTest::gtest_main protocol)

include(GoogleTest)
gtest_discover_tests(exchange_tests)
gtest_discover_tests(oms_tests)
gtest_discover_tests(order_coordinator_tests)
gtest_discover_tests(server_integration_tests)
