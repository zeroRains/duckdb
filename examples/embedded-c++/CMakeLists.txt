cmake_minimum_required(VERSION 2.8.12)
project(example-c++)


set(pybind11_DIR /root/db/origin/duckdb_origin/tools/pythonpkg/.eggs/pybind11-2.12.0-py3.12.egg/pybind11/share/cmake/pybind11)
set(PYTHON_INCLUDE_DIRS /root/miniconda3/envs/3.12/include/python3.12)
set(PYTHON_LIBRARIES /root/miniconda3/envs/3.12/lib)
find_package(pybind11 REQUIRED)
include_directories(${pybind11_INCLUDE_DIR})
include_directories(${PYTHON_INCLUDE_DIRS})


set(CMAKE_CXX_STANDARD 11)

add_executable(mul3 sub_interpreter.cpp)
target_include_directories(mul3 PUBLIC ${PYTHON_INCLUDE_DIRS})
target_link_libraries(mul3  PUBLIC ${PYTHON_LIBRARIES} pthread)

include_directories(../../src/include)
link_directories(../../build/release/src)
# link_directories(../../build/src)

include_directories(/root/origin/tmp/include)
link_directories(/root/origin/tmp/lib)


add_executable(example main.cpp)
target_link_libraries(example duckdb ${PYTHON_LIBRARIES})
add_executable(tpchq6_udf tpchq6_udf.cpp)
target_link_libraries(tpchq6_udf duckdb ${PYTHON_LIBRARIES})
add_executable(udf2 udf2.cpp)
target_link_libraries(udf2 duckdb ${PYTHON_LIBRARIES})
add_executable(udf3 udf3.cpp)
target_link_libraries(udf3 duckdb ${PYTHON_LIBRARIES})

add_executable(example_lightgbm example_lightgbm.cpp)
target_link_libraries(example_lightgbm _lightgbm ${PYTHON_LIBRARIES})

add_executable(udf_pf7 udf_pf7.cpp)
target_link_libraries(udf_pf7 _lightgbm duckdb ${PYTHON_LIBRARIES})

