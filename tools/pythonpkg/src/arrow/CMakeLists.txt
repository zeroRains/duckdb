# this is used for clang-tidy checks
include_directories(${pybind11_INCLUDE_DIR})
include_directories(${PYTHON_INCLUDE_DIRS})
find_package(pybind11 REQUIRED)
find_package(Arrow REQUIRED)

include_directories(${Arrow_INCLUDE_DIRS})

add_library(python_arrow OBJECT arrow_array_stream.cpp arrow_export_utils.cpp arrow_transform_utils.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:python_arrow>
    PARENT_SCOPE)
