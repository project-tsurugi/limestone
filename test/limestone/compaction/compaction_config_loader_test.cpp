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
#include "compaction_config_loader.h"

#include <cstdlib>
#include <limits>
#include <memory>
#include <string>

#include <boost/filesystem.hpp>

#include <limestone/api/limestone_exception.h>

#include "test_root.h"

#include "datastore_impl.h"

namespace limestone::testing {

using limestone::api::exception_type;
using limestone::api::limestone_exception;
using limestone::internal::apply_compaction_config;
using limestone::internal::compaction_config_parse_result;
using limestone::internal::compaction_settings;
using limestone::internal::load_compaction_config_from_environment;

class compaction_config_loader_test : public ::testing::Test {
protected:
    static constexpr const char* location = "/tmp/compaction_config_loader_test";
    static constexpr const char* threshold_env = "LIMESTONE_AUTO_COMPACTION_THRESHOLD_MB";
    static constexpr const char* delete_detached_env = "LIMESTONE_AUTO_COMPACTION_DELETE_DETACHED";
    static constexpr const char* blob_gc_env = "LIMESTONE_BLOB_GC";

    void SetUp() override {
        clear_env();
        boost::filesystem::remove_all(location);
        ASSERT_TRUE(boost::filesystem::create_directory(location));
    }

    void TearDown() override {
        clear_env();
        boost::filesystem::remove_all(location);
    }

    static void clear_env() {
        unsetenv(threshold_env);
        unsetenv(delete_detached_env);
        unsetenv(blob_gc_env);
    }

    static void set_env(const char* name, const char* value) {
        setenv(name, value, 1);
    }

    static limestone::api::configuration make_conf() {
        limestone::api::configuration conf{};
        conf.set_data_location(location);
        return conf;
    }

    // datastore を構築して、決まった常時コンパクションの設定を返す
    static compaction_settings settings_of(const limestone::api::configuration& conf) {
        std::unique_ptr<limestone::api::datastore_test> datastore =
            std::make_unique<limestone::api::datastore_test>(conf);
        return datastore->get_impl()->get_compaction_settings();
    }

    constexpr static std::uint64_t mib = std::uint64_t{1024} * 1024;
};

// 環境変数がどれも設定されていないときは、どの項目も未設定になる
TEST_F(compaction_config_loader_test, nothing_set) {
    const compaction_config_parse_result result = load_compaction_config_from_environment();

    ASSERT_TRUE(result.ok);
    EXPECT_FALSE(result.config.threshold_mb.has_value());
    EXPECT_FALSE(result.config.delete_detached.has_value());
    EXPECT_FALSE(result.config.blob_gc.has_value());
}

// 設定された値を読む。真偽の項目は、0 が無効、0 以外が有効である
TEST_F(compaction_config_loader_test, values_are_read) {
    set_env(threshold_env, "512");
    set_env(delete_detached_env, "0");
    set_env(blob_gc_env, "2");

    const compaction_config_parse_result result = load_compaction_config_from_environment();

    ASSERT_TRUE(result.ok);
    EXPECT_EQ(result.config.threshold_mb, std::optional<std::uint64_t>{512});
    EXPECT_EQ(result.config.delete_detached, std::optional<bool>{false});
    EXPECT_EQ(result.config.blob_gc, std::optional<bool>{true});
}

// 10 進の非負の整数でない値は、どの変数でも失敗になる
TEST_F(compaction_config_loader_test, invalid_values_fail) {
    for (const char* name : {threshold_env, delete_detached_env, blob_gc_env}) {
        for (const char* value : {"", "abc", "-1", "+1", "1.5", " 1", "1 ", "true", "99999999999999999999999"}) {
            clear_env();
            set_env(name, value);

            const compaction_config_parse_result result = load_compaction_config_from_environment();

            EXPECT_FALSE(result.ok) << name << "=" << value;
            EXPECT_NE(result.error_message.find(name), std::string::npos) << result.error_message;
        }
    }
}

// 読んだ設定のうち、設定されている項目だけが configuration に入る
TEST_F(compaction_config_loader_test, apply_puts_only_the_items_set) {
    set_env(threshold_env, "3");
    const compaction_config_parse_result result = load_compaction_config_from_environment();
    ASSERT_TRUE(result.ok);
    clear_env();

    limestone::api::configuration conf = make_conf();
    apply_compaction_config(result.config, conf);
    const compaction_settings settings = settings_of(conf);

    EXPECT_EQ(settings.threshold_bytes, 3 * mib);
    EXPECT_TRUE(settings.delete_detached);
    EXPECT_TRUE(settings.blob_gc);
}

// 読んだ設定の 3 項目とも設定されているときは、3 項目とも configuration に入る
TEST_F(compaction_config_loader_test, apply_puts_all_the_items) {
    set_env(threshold_env, "3");
    set_env(delete_detached_env, "0");
    set_env(blob_gc_env, "0");
    const compaction_config_parse_result result = load_compaction_config_from_environment();
    ASSERT_TRUE(result.ok);
    clear_env();

    limestone::api::configuration conf = make_conf();
    apply_compaction_config(result.config, conf);
    const compaction_settings settings = settings_of(conf);

    EXPECT_EQ(settings.threshold_bytes, 3 * mib);
    EXPECT_FALSE(settings.delete_detached);
    EXPECT_FALSE(settings.blob_gc);
}

// 何も設定されていないときの既定は、常時コンパクションが無効、削除と GC が有効、測定の間隔が 10 秒である
TEST_F(compaction_config_loader_test, datastore_defaults) {
    const compaction_settings settings = settings_of(make_conf());

    EXPECT_EQ(settings.threshold_bytes, 0U);
    EXPECT_TRUE(settings.delete_detached);
    EXPECT_TRUE(settings.blob_gc);
    EXPECT_EQ(settings.measurement_interval, std::chrono::seconds(10));
}

// configuration に設定されていない項目は、datastore が環境変数から読む
TEST_F(compaction_config_loader_test, datastore_reads_environment) {
    set_env(threshold_env, "16");
    set_env(delete_detached_env, "0");
    set_env(blob_gc_env, "0");

    const compaction_settings settings = settings_of(make_conf());

    EXPECT_EQ(settings.threshold_bytes, 16 * mib);
    EXPECT_FALSE(settings.delete_detached);
    EXPECT_FALSE(settings.blob_gc);
}

// configuration に設定した値は、環境変数より優先される。設定していない項目だけが環境変数から決まる
TEST_F(compaction_config_loader_test, configuration_takes_precedence) {
    set_env(threshold_env, "16");
    set_env(delete_detached_env, "0");

    limestone::api::configuration conf = make_conf();
    conf.set_auto_compaction_threshold_mb(4);
    const compaction_settings settings = settings_of(conf);

    EXPECT_EQ(settings.threshold_bytes, 4 * mib);
    EXPECT_FALSE(settings.delete_detached);
    EXPECT_TRUE(settings.blob_gc);
}

// 環境変数の値が誤っているときは、datastore の構築が失敗する
TEST_F(compaction_config_loader_test, datastore_fails_with_invalid_environment) {
    set_env(threshold_env, "-1");

    try {
        std::unique_ptr<limestone::api::datastore_test> datastore =
            std::make_unique<limestone::api::datastore_test>(make_conf());
        FAIL() << "the construction must fail";
    } catch (const limestone_exception& e) {
        EXPECT_EQ(e.type(), exception_type::initialization_failure);
        EXPECT_NE(std::string(e.what()).find(threshold_env), std::string::npos) << e.what();
    }
}

// 閾値が大きすぎて、バイトに直すと桁あふれするときは、datastore の構築が失敗する
TEST_F(compaction_config_loader_test, datastore_fails_with_too_large_threshold) {
    limestone::api::configuration conf = make_conf();
    conf.set_auto_compaction_threshold_mb(std::numeric_limits<std::size_t>::max());

    try {
        std::unique_ptr<limestone::api::datastore_test> datastore =
            std::make_unique<limestone::api::datastore_test>(conf);
        FAIL() << "the construction must fail";
    } catch (const limestone_exception& e) {
        EXPECT_EQ(e.type(), exception_type::initialization_failure);
        EXPECT_NE(std::string(e.what()).find("auto_compaction_threshold_mb is too large"), std::string::npos)
            << e.what();
    }
}

// 3 項目とも configuration に設定されているときは、環境変数を読まない
TEST_F(compaction_config_loader_test, environment_is_not_read_when_all_items_are_set) {
    set_env(threshold_env, "invalid");

    limestone::api::configuration conf = make_conf();
    conf.set_auto_compaction_threshold_mb(1);
    conf.set_auto_compaction_delete_detached(false);
    conf.set_blob_gc(false);
    const compaction_settings settings = settings_of(conf);

    EXPECT_EQ(settings.threshold_bytes, 1 * mib);
    EXPECT_FALSE(settings.delete_detached);
    EXPECT_FALSE(settings.blob_gc);
}

}  // namespace limestone::testing
