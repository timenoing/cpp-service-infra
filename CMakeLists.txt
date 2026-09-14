cmake_minimum_required(VERSION 3.10)
project(cpp-service-infra)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 重点！加上 ${PROJECT_SOURCE_DIR} 项目根目录！
include_directories(
    ${PROJECT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/src
)

set(COMMON_SRC
    src/Connection.cpp
    src/ConnectionGuard.cpp
    src/ConnectionPool.cpp
    src/Cursor.cpp
    src/Epoll.cpp
    src/Logger.cpp
    src/ThreadPool.cpp
    src/Trace.cpp
    src/Message.cpp
)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
add_executable(server 9.2main.cpp ${COMMON_SRC})
find_package(SQLite3 REQUIRED)
target_link_libraries(server PRIVATE SQLite::SQLite3)

add_executable(message_test test/message_test.cpp src/Message.cpp)
