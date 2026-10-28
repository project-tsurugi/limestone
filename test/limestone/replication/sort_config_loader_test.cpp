/*
 * Copyright 2022-2026 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "replication/sort_config_loader.h"

#include <cstdlib>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "test_root.h"
#include <limestone/api/limestone_exception.h>

namespace limestone::testing {

using limestone::replication::apply_sort_config;
using limestone::replication::load_sort_config_from_environment;

class sort_config_loader_test : public ::testing::Test {
protected:
    void SetUp() override {
        clear_env();
    }

    void TearDown() override {
        clear_env();
    }

    static void clear_env() {
        unsetenv("LIMESTONE_SORT_THREADS");
        unsetenv("LIMESTONE_SORT_ARENA_SIZE_MB");
        unsetenv("LIMESTONE_SORT_WORK_DIR");
        unsetenv("LIMESTONE_SORT_MERGE_FANIN");
    }

    static void set_env(char const* name, char const* value) {
        setenv(name, value, 1);
    }

    static limestone::api::configuration make_conf() {
        if (system("rm -rf /tmp/sort_config_loader_test") != 0) {
            ADD_FAILURE() << "cannot remove directory";
        }
        if (system("mkdir -p /tmp/sort_config_loader_test") != 0) {
            ADD_FAILURE() << "cannot make directory";
        }
        limestone::api::configuration conf{};
        conf.set_data_location("/tmp/sort_config_loader_test");
        return conf;
    }
};

// without the environment variables, everything is unset (nullopt)
TEST_F(sort_config_loader_test, no_env_leaves_everything_unset) {
    auto const result = load_sort_config_from_environment();
    ASSERT_TRUE(result.ok) << result.error_message;
    EXPECT_FALSE(result.config.threads.has_value());
    EXPECT_FALSE(result.config.arena_size_mb.has_value());
    EXPECT_FALSE(result.config.work_dir.has_value());
    EXPECT_FALSE(result.config.merge_fanin.has_value());
}

// the values of the four variables are read
TEST_F(sort_config_loader_test, reads_the_four_variables) {
    set_env("LIMESTONE_SORT_THREADS", "4");
    set_env("LIMESTONE_SORT_ARENA_SIZE_MB", "64");
    set_env("LIMESTONE_SORT_WORK_DIR", "/tmp/sort_config_loader_test/work");
    set_env("LIMESTONE_SORT_MERGE_FANIN", "16");
    auto const result = load_sort_config_from_environment();
    ASSERT_TRUE(result.ok) << result.error_message;
    EXPECT_EQ(result.config.threads, 4);
    EXPECT_EQ(result.config.arena_size_mb, 64U);
    EXPECT_EQ(result.config.work_dir, "/tmp/sort_config_loader_test/work");
    EXPECT_EQ(result.config.merge_fanin, 16U);
}

// an empty LIMESTONE_SORT_WORK_DIR is the same as unset
TEST_F(sort_config_loader_test, empty_work_dir_is_unset) {
    set_env("LIMESTONE_SORT_WORK_DIR", "");
    auto const result = load_sort_config_from_environment();
    ASSERT_TRUE(result.ok) << result.error_message;
    EXPECT_FALSE(result.config.work_dir.has_value());
}

// a value that is not a number (letters, a sign, empty, THREADS beyond int) fails with the name of the variable
TEST_F(sort_config_loader_test, malformed_numbers_are_rejected) {
    struct malformed {
        char const* name;
        char const* value;
    };
    for (const malformed& m : {malformed{"LIMESTONE_SORT_THREADS", "abc"}, malformed{"LIMESTONE_SORT_THREADS", "-1"},
                               malformed{"LIMESTONE_SORT_THREADS", "2147483648"},
                               malformed{"LIMESTONE_SORT_ARENA_SIZE_MB", ""},
                               malformed{"LIMESTONE_SORT_MERGE_FANIN", "16x"}}) {
        clear_env();
        set_env(m.name, m.value);
        auto const result = load_sort_config_from_environment();
        EXPECT_FALSE(result.ok) << m.name << "=" << m.value;
        EXPECT_NE(result.error_message.find(m.name), std::string::npos) << result.error_message;
    }
}

// the values put by apply_sort_config() reach the datastore: with a value out of range, the construction of the
// datastore fails at the check of that item
TEST_F(sort_config_loader_test, applied_values_reach_the_datastore) {
    struct rejected {
        char const* name;
        char const* value;
        char const* message;
    };
    const std::vector<rejected> cases{
        {"LIMESTONE_SORT_THREADS", "0", "recover_max_parallelism must be at least 1: 0"},
        {"LIMESTONE_SORT_ARENA_SIZE_MB", "0", "sort_arena_size_mb must be between 1 and 4096: 0"},
        {"LIMESTONE_SORT_WORK_DIR", "/tmp/sort_config_loader_test/missing",
         "sort_work_dir is not an existing directory: /tmp/sort_config_loader_test/missing"},
        {"LIMESTONE_SORT_MERGE_FANIN", "1", "sort_merge_fanin must be at least 2: 1"},
    };
    for (const rejected& r : cases) {
        clear_env();
        set_env(r.name, r.value);
        auto const result = load_sort_config_from_environment();
        ASSERT_TRUE(result.ok) << result.error_message;
        auto conf = make_conf();
        apply_sort_config(result.config, conf);
        try {
            limestone::api::datastore_test ds{conf};
            FAIL() << "no exception: " << r.name << "=" << r.value;
        } catch (const limestone::api::limestone_exception& e) {
            EXPECT_NE(std::string(e.what()).find(r.message), std::string::npos) << e.what();
        }
    }
    // valid values construct
    clear_env();
    set_env("LIMESTONE_SORT_THREADS", "2");
    set_env("LIMESTONE_SORT_ARENA_SIZE_MB", "1");
    set_env("LIMESTONE_SORT_MERGE_FANIN", "2");
    auto const result = load_sort_config_from_environment();
    ASSERT_TRUE(result.ok) << result.error_message;
    auto conf = make_conf();
    apply_sort_config(result.config, conf);
    limestone::api::datastore_test ds{conf};
}

}  // namespace limestone::testing
