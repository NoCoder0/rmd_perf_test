cmake_minimum_required(VERSION 3.16)

project(rdma_600_stage1 LANGUAGES CXX)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "rdma_600 targets the Linux RDMA hosts; configure it on the deployment machine.")
endif()

set(HCOM_INCLUDE_DIR "" CACHE PATH "Directory containing hcom/hcom_service.h")
set(HCOM_LIB_DIR "" CACHE PATH "Directory containing libhcom_static.a")
set(HCOM_3RDPARTY_DIR "" CACHE PATH "ubs-comm dist/hcom_3rdparty directory")

if(NOT HCOM_INCLUDE_DIR OR NOT EXISTS "${HCOM_INCLUDE_DIR}/hcom/hcom_service.h")
    message(FATAL_ERROR "Set -DHCOM_INCLUDE_DIR=/absolute/path/to/dist/hcom/include")
endif()
if(NOT HCOM_LIB_DIR)
    message(FATAL_ERROR "Set -DHCOM_LIB_DIR=/absolute/path/to/dist/hcom/lib")
endif()

if(NOT HCOM_3RDPARTY_DIR)
    get_filename_component(_hcom_dir "${HCOM_INCLUDE_DIR}" DIRECTORY)
    get_filename_component(_dist_dir "${_hcom_dir}" DIRECTORY)
    set(HCOM_3RDPARTY_DIR "${_dist_dir}/hcom_3rdparty")
endif()

find_library(HCOM_STATIC_LIBRARY
    NAMES hcom_static
    PATHS "${HCOM_LIB_DIR}"
    NO_DEFAULT_PATH)
find_path(BOUNDSCHECK_INCLUDE_DIR
    NAMES securec.h
    PATHS "${HCOM_3RDPARTY_DIR}/libboundscheck/include"
    NO_DEFAULT_PATH)
find_library(BOUNDSCHECK_LIBRARY
    NAMES boundscheck libboundscheck
    PATHS "${HCOM_3RDPARTY_DIR}/libboundscheck/lib"
    NO_DEFAULT_PATH)
if(NOT HCOM_STATIC_LIBRARY)
    message(FATAL_ERROR "libhcom_static.a was not found below ${HCOM_LIB_DIR}")
endif()
if(NOT BOUNDSCHECK_INCLUDE_DIR OR NOT BOUNDSCHECK_LIBRARY)
    message(FATAL_ERROR "libboundscheck headers/library were not found below ${HCOM_3RDPARTY_DIR}")
endif()

set(URMA_INCLUDE_DIR "${HCOM_3RDPARTY_DIR}/umdk/urma/include" CACHE PATH "URMA header directory")
if(NOT EXISTS "${URMA_INCLUDE_DIR}")
    message(FATAL_ERROR "Set -DURMA_INCLUDE_DIR=/absolute/path/to/umdk/urma/include")
endif()

find_package(Threads REQUIRED)
find_library(RT_LIBRARY NAMES rt)

execute_process(
    COMMAND git -C "${CMAKE_CURRENT_SOURCE_DIR}" rev-parse --short HEAD
    OUTPUT_VARIABLE RDMA_600_GIT_COMMIT
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET)
if(NOT RDMA_600_GIT_COMMIT)
    set(RDMA_600_GIT_COMMIT "unknown")
endif()

add_executable(rdma_600 rdma_600.cpp)
target_compile_features(rdma_600 PRIVATE cxx_std_17)
target_compile_options(rdma_600 PRIVATE -Wall -Wextra -Wpedantic -Werror=return-type)
target_compile_definitions(rdma_600 PRIVATE
    _GNU_SOURCE
    "RDMA_600_GIT_COMMIT=\"${RDMA_600_GIT_COMMIT}\"")
target_include_directories(rdma_600 PRIVATE
    "${HCOM_INCLUDE_DIR}"
    "${BOUNDSCHECK_INCLUDE_DIR}"
    "${URMA_INCLUDE_DIR}")
target_link_libraries(rdma_600 PRIVATE
    "${HCOM_STATIC_LIBRARY}"
    Threads::Threads
    "${CMAKE_DL_LIBS}"
    "${BOUNDSCHECK_LIBRARY}")
if(RT_LIBRARY)
    target_link_libraries(rdma_600 PRIVATE "${RT_LIBRARY}")
endif()

# The runner also sets LD_LIBRARY_PATH and records it.  This build-tree RPATH
# simply makes a manual local smoke run resolve the exact HCOM artifacts passed
# to CMake, rather than a similarly named system copy.
set_target_properties(rdma_600 PROPERTIES
    BUILD_RPATH "${HCOM_LIB_DIR};${HCOM_3RDPARTY_DIR}/libboundscheck/lib"
    INSTALL_RPATH "")

message(STATUS "HCOM include: ${HCOM_INCLUDE_DIR}")
message(STATUS "HCOM static library: ${HCOM_STATIC_LIBRARY}")
message(STATUS "HCOM third-party directory: ${HCOM_3RDPARTY_DIR}")
