cmake_minimum_required(VERSION 3.5)

if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

project(imdb CXX)
include_directories(${pybind11_INCLUDE_DIR})
include_directories(${PYTHON_INCLUDE_DIRS})
find_package(pybind11 REQUIRED)
include_directories(include)

set(CMAKE_BUILD_TYPE "Release")
add_library(imdb STATIC imdb.cpp ${ALL_OBJECT_FILES})

disable_target_warnings(imdb)
