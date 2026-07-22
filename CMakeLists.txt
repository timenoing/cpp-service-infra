cmake_minimum_required(VERSION 3.10)
project(cpp-service-infra)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 头文件路径
include_directories(include)

# 收集src目录下所有cpp源码
include_directories(${PROJECT_SOURCE_DIR}/include)
aux_source_directory(./src SRC_LIST)

# 编译产物输出到bin文件夹
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)

# 生成可执行文件，名字统一用main
add_executable(main main.cpp ${SRC_LIST})