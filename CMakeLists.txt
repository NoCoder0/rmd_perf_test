cmake_minimum_required(VERSION 3.16)

project(rdma_600_sparse_copy_dual_rail LANGUAGES CXX)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "rdma_600 targets the Linux RDMA hosts; configure it on the deployment machine.")
endif()

set(HCOM_INCLUDE_DIR "" CACHE PATH "Directory containing hcom/hcom_service.h; provide with -D or build.sh")
set(HCOM_LIB_DIR "" CACHE PATH "Directory containing libhcom_static.a; provide with -D or build.sh")
set(BOUNDSCHECK_ROOT "" CACHE PATH "Directory containing libboundscheck include/ and lib/; provide with -D or build.sh")

if(NOT HCOM_INCLUDE_DIR OR
   NOT EXISTS "${HCOM_INCLUDE_DIR}/hcom/hcom_service.h" OR
   NOT EXISTS "${HCOM_INCLUDE_DIR}/hcom/hcom_service_context.h")
    message(FATAL_ERROR
        "HCOM_INCLUDE_DIR must contain hcom/hcom_service.h and hcom/hcom_service_context.h; "
        "pass -DHCOM_INCLUDE_DIR=... or run bash ./build.sh --ubs-root /path/to/ubs-comm")
endif()
if(NOT EXISTS "${HCOM_INCLUDE_DIR}/hcom/hcom_rdma_trace.h")
    message(FATAL_ERROR "HCOM diagnostic trace header is missing. Rebuild/install the updated ubs-comm first.")
endif()
if(NOT HCOM_LIB_DIR)
    message(FATAL_ERROR "HCOM_LIB_DIR is required; pass -DHCOM_LIB_DIR=... or run bash ./build.sh --ubs-root /path/to/ubs-comm")
endif()

if(NOT BOUNDSCHECK_ROOT)
    get_filename_component(_hcom_dir "${HCOM_INCLUDE_DIR}" DIRECTORY)
    get_filename_component(_dist_dir "${_hcom_dir}" DIRECTORY)
    set(BOUNDSCHECK_ROOT "${_dist_dir}/hcom_3rdparty/libboundscheck")
endif()

# find_library/find_path cache successful lookups. Clear only these derived
# lookup results so reconfiguring one build directory with a new --ubs-root
# cannot keep linking headers or libraries from the old deployment.
unset(HCOM_STATIC_LIBRARY)
unset(HCOM_STATIC_LIBRARY CACHE)
unset(BOUNDSCHECK_INCLUDE_DIR)
unset(BOUNDSCHECK_INCLUDE_DIR CACHE)
unset(BOUNDSCHECK_LIBRARY)
unset(BOUNDSCHECK_LIBRARY CACHE)
find_file(HCOM_STATIC_LIBRARY
    NAMES libhcom_static.a
    PATHS "${HCOM_LIB_DIR}"
    NO_DEFAULT_PATH)
find_path(BOUNDSCHECK_INCLUDE_DIR
    NAMES securec.h
    PATHS "${BOUNDSCHECK_ROOT}/include"
    NO_DEFAULT_PATH)
find_library(BOUNDSCHECK_LIBRARY
    NAMES boundscheck
    PATHS "${BOUNDSCHECK_ROOT}/lib"
    NO_DEFAULT_PATH)
if(NOT HCOM_STATIC_LIBRARY)
    message(FATAL_ERROR "libhcom_static.a was not found below ${HCOM_LIB_DIR}")
endif()
if(NOT BOUNDSCHECK_INCLUDE_DIR OR NOT BOUNDSCHECK_LIBRARY)
    message(FATAL_ERROR "securec.h or libboundscheck was not found below ${BOUNDSCHECK_ROOT}")
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
else()
    execute_process(
        COMMAND git -C "${CMAKE_CURRENT_SOURCE_DIR}" diff --quiet HEAD --
        RESULT_VARIABLE RDMA_600_GIT_DIFF_RESULT
        ERROR_QUIET)
    if(RDMA_600_GIT_DIFF_RESULT EQUAL 1)
        string(APPEND RDMA_600_GIT_COMMIT "-dirty")
    elseif(NOT RDMA_600_GIT_DIFF_RESULT EQUAL 0)
        string(APPEND RDMA_600_GIT_COMMIT "-status-unknown")
    endif()
endif()

add_executable(rdma_600
    rdma_600.cpp
    src/aligned_buffer.cpp
    src/mapping_snapshot.cpp
    src/config.cpp
    src/support.cpp
    src/protocol.cpp
    src/data_path.cpp
    src/benchmark.cpp
    src/local.cpp
    src/remote.cpp
    src/control.cpp
    src/transport.cpp
    src/results.cpp)
target_sources(rdma_600 PRIVATE src/detailed_trace.cpp)
target_compile_features(rdma_600 PRIVATE cxx_std_17)
target_compile_options(rdma_600 PRIVATE -Wall -Wextra -Wpedantic -Werror=return-type)
target_compile_definitions(rdma_600 PRIVATE
    "RDMA_600_GIT_COMMIT=\"${RDMA_600_GIT_COMMIT}\""
    "RDMA_600_BUILD_TYPE=\"${CMAKE_BUILD_TYPE}\"")
target_include_directories(rdma_600 PRIVATE
    "${HCOM_INCLUDE_DIR}"
    "${BOUNDSCHECK_INCLUDE_DIR}")
target_link_libraries(rdma_600 PRIVATE
    "${HCOM_STATIC_LIBRARY}"
    Threads::Threads
    "${CMAKE_DL_LIBS}"
    "${BOUNDSCHECK_LIBRARY}")
if(RT_LIBRARY)
    target_link_libraries(rdma_600 PRIVATE "${RT_LIBRARY}")
endif()

message(STATUS "HCOM include: ${HCOM_INCLUDE_DIR}")
message(STATUS "HCOM static library: ${HCOM_STATIC_LIBRARY}")
message(STATUS "boundscheck include: ${BOUNDSCHECK_INCLUDE_DIR}")
message(STATUS "boundscheck library: ${BOUNDSCHECK_LIBRARY}")
