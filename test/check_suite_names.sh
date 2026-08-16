#!/bin/bash
# Test-suite naming convention check.
#
# Each ctest entry runs "only this file's tests" by using the test source's
# file name as a gtest_filter (add_test_executable in CMakeLists.txt). CMake
# never looks inside the files, so this selection relies on the convention
# that the suite name (the first argument of TEST / TEST_F / TEST_P) equals
# the file name. A suite that breaks the convention is selected by no entry's
# filter and stays unexecuted while everything looks green. This script
# verifies the convention mechanically.
set -u
cd "$(dirname "$0")" || exit 1
status=0
checked=0
while IFS= read -r file; do
    checked=$((checked + 1))
    base=$(basename "$file" .cpp)
    while IFS= read -r suite; do
        if [ "$suite" != "$base" ]; then
            echo "NG: ${file}: suite name '${suite}' does not match the file name '${base}'"
            status=1
        fi
    done < <(grep -hoP '^\s*TEST(_[FP])?\s*\(\s*\K[A-Za-z0-9_]+' "$file" | sort -u)
    # A TEST macro opened at the end of a line (suite name on the next line)
    # cannot be checked by the line-based extraction. A silent miss would end
    # the same way as an unchecked violation, so fail loudly as uncheckable.
    if grep -nP '^\s*TEST(_[FP])?\s*\(\s*$' "$file"; then
        echo "NG: ${file}: TEST macro is split across lines and cannot be checked (write it on one line)"
        status=1
    fi
done < <(find limestone -name '*.cpp' | sort)
if [ "$checked" -eq 0 ]; then
    echo "NG: no test source found to check (verify the execution directory)"
    exit 1
fi
if [ "$status" -ne 0 ]; then
    echo "Test suite names must match their file name (one suite per file)."
    echo "A mismatched suite is executed by no ctest entry."
fi
exit $status
