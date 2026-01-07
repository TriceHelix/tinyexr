cmake_minimum_required(VERSION 3.28 FATAL_ERROR)

project(
    tinyexr VERSION 1.0.13
    LANGUAGES C CXX
)

add_library(
    tinyexr OBJECT
    "tinyexr.cc"
    "deps/miniz/miniz.c"
)

set_target_properties(
    tinyexr PROPERTIES
    C_STANDARD 11
    C_STANDARD_REQUIRED TRUE
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED TRUE
)

target_include_directories(
    tinyexr PUBLIC
    "${PROJECT_SOURCE_DIR}"
    "${PROJECT_SOURCE_DIR}/deps/miniz"
)
