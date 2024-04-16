cmake_minimum_required(VERSION 2.8.12)
project(example-c++)

set(CMAKE_CXX_STANDARD 11)

include_directories(../../src/include)
link_directories(../../build/src)

add_executable(example main.cpp)
target_link_libraries(example duckdb)
add_executable(udf2 udf2.cpp)
target_link_libraries(udf2 duckdb)
add_executable(udf3 udf3.cpp)
target_link_libraries(udf3 duckdb)
