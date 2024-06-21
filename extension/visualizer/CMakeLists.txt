cmake_minimum_required(VERSION 2.8.12)

project(VisualizerExtension)
include_directories(${pybind11_INCLUDE_DIR})
include_directories(${PYTHON_INCLUDE_DIRS})
find_package(pybind11 REQUIRED)
include_directories(include)

build_static_extension(visualizer visualizer_extension.cpp)
set(PARAMETERS "-warnings")
build_loadable_extension(visualizer ${PARAMETERS} visualizer_extension.cpp)
install(
  TARGETS visualizer_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
