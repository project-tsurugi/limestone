#!/bin/bash
#
# Runs clang-tidy in the same container image as the Analysis job of the CI (.github/workflows/ci-build.yml).
#
# usage: scripts/run-clang-tidy-ci.sh [file-regex ...]
#
#   Without file-regex, all files under src are analyzed, as in the CI.
#   The result is written to build-tidy/clang-tidy.log, and the exit status is 1 if there are warnings.
#
# Environment variables
#   TIDY_IMAGE      container image (default: ghcr.io/project-tsurugi/tsurugi-ci:almalinux-latest, the same as the CI)
#   TIDY_BUILD_DIR  build directory name (default: build-tidy)
#   TIDY_NO_CACHE   if 1, analyzes all files without ctcache
#
# ctcache keys the results by the preprocessed source, so a change only in preprocessor directives (such as
# #if defined X to #ifdef X) keeps the key and returns the result before the change. Run with TIDY_NO_CACHE=1 to
# check such a change.

set -euo pipefail

IMAGE=${TIDY_IMAGE:-ghcr.io/project-tsurugi/tsurugi-ci:almalinux-latest}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
BUILD_DIR=${TIDY_BUILD_DIR:-build-tidy}

if [ "$#" -eq 0 ]; then
    set -- "${ROOT}/src/.*"
fi

docker pull "${IMAGE}"

LOG="${ROOT}/${BUILD_DIR}/clang-tidy.log"
rm -f "${LOG}"

# Mount the repository at the same path in the container so that the paths in compile_commands.json can be used
# as they are, and run as the host user so that no root-owned files are created.
# As in the CI, clang-tidy is called through ctcache, which caches the results in the build directory. The comments
# are included in the key (CTCACHE_KEEP_COMMENTS) so that adding or removing NOLINT changes the key.
status=0
docker run --rm \
    --init \
    -e TINI_KILL_PROCESS_GROUP=1 \
    --user "$(id -u):$(id -g)" \
    -v "${ROOT}:${ROOT}" \
    -w "${ROOT}" \
    -e HOME=/tmp \
    -e CC=clang \
    -e CXX=clang++ \
    -e CTCACHE_LOCAL=1 \
    -e CTCACHE_SAVE_OUTPUT=1 \
    -e CTCACHE_KEEP_COMMENTS=1 \
    -e CTCACHE_DISABLE="${TIDY_NO_CACHE:-0}" \
    -e CTCACHE_DIR="${ROOT}/${BUILD_DIR}/ctcache" \
    "${IMAGE}" \
    bash -euo pipefail -c '
        root=$1
        build_dir=$2
        shift 2
        if [ ! -f "${build_dir}/build.ninja" ]; then
            cmake -S . -B "${build_dir}" -G Ninja -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=OFF -DBUILD_STRICT=OFF \
                -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
        fi
        cmake --build "${build_dir}" --target all
        clang-tidy --version
        # The analysis output goes to the log file, so show the progress (the number of analyzed files) separately.
        total=$(python3 -c "import json, re, sys
files = [e[\"file\"] for e in json.load(open(sys.argv[1]))]
print(sum(1 for f in files if any(re.search(p, f) for p in sys.argv[2:])))" "${build_dir}/compile_commands.json" "$@")
        echo "running clang-tidy on ${total} files"
        python3 -u tools/bin/run-clang-tidy.py -clang-tidy-binary=/opt/ctcache/clang-tidy -quiet -p "${build_dir}" \
            -extra-arg=-Wno-unknown-warning-option -header-filter="${root}/(include|src)/.*\.h$" "$@" 2>&1 \
            | awk -v total="${total}" -v out="${build_dir}/clang-tidy.log" "
                /^[^ ]*clang-tidy /{ n++; printf \"  [%d/%d] %s\\n\", n, total, \$NF > \"/dev/stderr\"; fflush(\"/dev/stderr\") }
                !a[\$0]++ { print > out }"
    ' bash "${ROOT}" "${BUILD_DIR}" "$@" || status=$?

COUNT=$(grep -c ' warning: ' "${LOG}" 2>/dev/null || true)
echo "warnings: ${COUNT:-0} (${LOG})"
# Print the warning and error lines for the problem matcher of the editor. The same header is also reported with a
# src/./ path, so normalize the paths and remove the duplicates.
grep -E ' (warning|error): ' "${LOG}" 2>/dev/null | sed 's#/\./#/#' | awk '!a[$0]++' || true
if [ "${COUNT:-0}" -ne 0 ]; then
    grep ' warning: ' "${LOG}" | grep -o '\[[a-z0-9.,-]*\]$' | sort | uniq -c | sort -rn
fi
if [ "${status}" -ne 0 ]; then
    echo "the build or clang-tidy failed (exit status ${status}); see the output above and ${LOG}" >&2
    exit "${status}"
fi
[ "${COUNT:-0}" -eq 0 ] || exit 1
