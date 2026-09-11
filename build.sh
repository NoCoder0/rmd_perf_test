#!/usr/bin/env bash
# Build rdma_600 against an existing ubs-comm / HCOM build.
set -euo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

usage() {
    cat <<'EOF'
Usage:
  bash ./build.sh --ubs-root <path> [options]
  UBS_ROOT=<path> bash ./build.sh [options]

Build rdma_600 against a previously built ubs-comm / HCOM tree. This script
does not build ubs-comm and must run on a Linux target host.

Options:
  --ubs-root <path>          ubs-comm root; derives all HCOM artifact paths.
  --hcom-include <path>      directory containing hcom/hcom_service.h.
  --hcom-lib <path>          directory containing libhcom_static.a.
  --boundscheck-root <path>  directory containing boundscheck include/ and lib/.
  --build-dir <path>         CMake build directory (default: ./build).
  --build-type <type>        CMake build type (default: Release).
  --jobs <count>             parallel build jobs (default: detected CPUs).
  -h, --help                 show this help text.
  --                         pass remaining arguments to CMake configure.

Examples:
  bash ./build.sh --ubs-root /opt/ubs-comm
  bash ./build.sh --ubs-root /opt/ubs-comm --build-type Debug --jobs 16
  bash ./build.sh --ubs-root /opt/ubs-comm -- -G Ninja
EOF
}

die() {
    printf 'error: %s\n' "$*" >&2
    exit 2
}

require_value() {
    local option="$1"
    [[ $# -ge 2 ]] || die "${option} requires a value"
}

require_directory() {
    local option="$1"
    local path="$2"
    [[ -d "$path" ]] || die "${option} directory does not exist: ${path}"
}

ubs_root="${UBS_ROOT:-}"
hcom_include="${HCOM_INCLUDE_DIR:-}"
hcom_lib="${HCOM_LIB_DIR:-}"
boundscheck_root="${BOUNDSCHECK_ROOT:-}"
build_dir="${BUILD_DIR:-${SCRIPT_DIR}/build}"
build_type="${CMAKE_BUILD_TYPE:-Release}"
jobs="${JOBS:-}"
cmake_extra_args=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ubs-root)
            require_value "$@"
            ubs_root="$2"
            shift 2
            ;;
        --hcom-include)
            require_value "$@"
            hcom_include="$2"
            shift 2
            ;;
        --hcom-lib)
            require_value "$@"
            hcom_lib="$2"
            shift 2
            ;;
        --boundscheck-root)
            require_value "$@"
            boundscheck_root="$2"
            shift 2
            ;;
        --build-dir)
            require_value "$@"
            build_dir="$2"
            shift 2
            ;;
        --build-type)
            require_value "$@"
            build_type="$2"
            shift 2
            ;;
        --jobs)
            require_value "$@"
            jobs="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            cmake_extra_args=("$@")
            break
            ;;
        *)
            die "unknown option: $1 (use --help)"
            ;;
    esac
done

if [[ -n "$ubs_root" ]]; then
    require_directory "--ubs-root" "$ubs_root"
    ubs_root="$(cd -- "$ubs_root" && pwd -P)"
    hcom_include="${hcom_include:-${ubs_root}/dist/hcom/include}"
    hcom_lib="${hcom_lib:-${ubs_root}/dist/hcom/lib}"
    boundscheck_root="${boundscheck_root:-${ubs_root}/dist/hcom_3rdparty/libboundscheck}"
fi

[[ -n "$hcom_include" ]] || die "provide --ubs-root or --hcom-include"
[[ -n "$hcom_lib" ]] || die "provide --ubs-root or --hcom-lib"

if [[ -z "$boundscheck_root" ]]; then
    hcom_dir="$(dirname -- "$hcom_include")"
    dist_dir="$(dirname -- "$hcom_dir")"
    boundscheck_root="${dist_dir}/hcom_3rdparty/libboundscheck"
fi

require_directory "--hcom-include" "$hcom_include"
require_directory "--hcom-lib" "$hcom_lib"
require_directory "--boundscheck-root" "$boundscheck_root"

hcom_include="$(cd -- "$hcom_include" && pwd -P)"
hcom_lib="$(cd -- "$hcom_lib" && pwd -P)"
boundscheck_root="$(cd -- "$boundscheck_root" && pwd -P)"

if [[ -z "$jobs" ]]; then
    if command -v nproc >/dev/null 2>&1; then
        jobs="$(nproc)"
    elif command -v getconf >/dev/null 2>&1; then
        jobs="$(getconf _NPROCESSORS_ONLN)"
    else
        jobs=1
    fi
fi
[[ "$jobs" =~ ^[1-9][0-9]*$ ]] || die "--jobs must be a positive integer"

if [[ "$(uname -s)" != "Linux" ]]; then
    die "rdma_600 must be built on a Linux RDMA host"
fi

cmake -S "$SCRIPT_DIR" -B "$build_dir" \
    "${cmake_extra_args[@]}" \
    -DCMAKE_BUILD_TYPE="$build_type" \
    -DHCOM_INCLUDE_DIR="$hcom_include" \
    -DHCOM_LIB_DIR="$hcom_lib" \
    -DBOUNDSCHECK_ROOT="$boundscheck_root"
cmake --build "$build_dir" --parallel "$jobs"
