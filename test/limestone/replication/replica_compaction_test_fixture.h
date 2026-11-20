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
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>

#include <limestone/api/cursor.h>
#include <limestone/api/snapshot.h>

#include "test_root.h"

#include "compaction_settings.h"
#include "compaction_catalog.h"
#include "datastore_impl.h"
#include "internal.h"
#include "locator/locator_format.h"
#include "log_channel_impl.h"

namespace limestone::testing {

using limestone::api::epoch_id_type;
using limestone::api::log_channel;
using limestone::internal::compaction_settings;
using limestone::internal::compaction_catalog;

/**
 * The fixture of the tests of the rotation and the compaction on the replica. The reception from the master is
 * imitated, without the network, by the calls the receive handlers make (the begin and the end of a session, and
 * the persistence of the epoch of GROUP_COMMIT followed by the update of the received epoch).
 */
class replica_compaction_base_test : public ::testing::Test {
public:
    explicit replica_compaction_base_test(const char* location) : location(location) {}

    const char* location;

    // The time to wait when checking that nothing runs. Longer than the period of the loop of the worker (1 second)
    static constexpr std::chrono::milliseconds quiet_period{1500};

    void SetUp() override {
        boost::filesystem::remove_all(location);
        ASSERT_TRUE(boost::filesystem::create_directory(location));
    }

    void TearDown() override {
        datastore_ = nullptr;
        boost::filesystem::remove_all(location);
    }

    // The settings with the threshold of one byte: writing one entry makes the amount of the WAL exceed it. The
    // measurement interval is short
    static compaction_settings eager_settings() {
        compaction_settings settings{};
        settings.threshold_bytes = 1;
        settings.measurement_interval = std::chrono::milliseconds(50);
        return settings;
    }

    // Creates the datastore of a replica. The startup processing is not performed yet
    void create_replica(const compaction_settings& settings) {
        limestone::api::configuration conf{};
        conf.set_data_location(location);
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
        datastore_->get_impl()->set_compaction_settings(settings);
        datastore_->get_impl()->set_replica_role();
    }

    void start_replica(const compaction_settings& settings) {
        create_replica(settings);
        datastore_->start_as_replica();
    }

    void stop() {
        datastore_->shutdown();
        datastore_ = nullptr;
    }

    // Imitates the reception of the logs of one session from the master
    static void receive_session(log_channel& channel, epoch_id_type epoch, const std::string& key,
                                const std::string& value) {
        channel.get_impl()->begin_session_at(epoch);
        channel.add_entry(1, key, value, {epoch, 0});
        channel.get_impl()->end_session_at(epoch);
    }

    // Imitates the reception of GROUP_COMMIT from the master
    void receive_group_commit(epoch_id_type epoch) {
        datastore_->persist_and_propagate_epoch_id(epoch);
        datastore_->get_impl()->set_received_epoch(epoch);
    }

    // The generation of the committed compaction (taken from the name of the compacted WAL file that the
    // datastore holds)
    [[nodiscard]] std::uint64_t generation() const {
        const std::optional<std::string> name = datastore_->get_impl()->get_current_compacted_file_name();
        if (! name.has_value()) {
            return 0;
        }
        for (std::uint64_t candidate = 1; candidate < 100; ++candidate) {
            if (name.value() == compaction_catalog::get_compacted_filename_for_generation(candidate)) {
                return candidate;
            }
        }
        ADD_FAILURE() << "unexpected name of the compacted WAL file: " << name.value();
        return 0;
    }

    [[nodiscard]] bool wait_for_generation(std::uint64_t expected, std::chrono::milliseconds timeout) const {
        const std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (generation() >= expected) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return generation() >= expected;
    }

    // The names of the rotated WAL files in the log directory
    [[nodiscard]] std::set<std::string> rotated_wal_files() const {
        std::set<std::string> result{};
        for (const boost::filesystem::directory_entry& entry :
             boost::filesystem::directory_iterator(boost::filesystem::path(location))) {
            const std::string name = entry.path().filename().string();
            if (name.rfind("pwal_", 0) == 0 && ! limestone::internal::is_unrotated_pwal_name(name)
                && ! compaction_catalog::is_compaction_output_filename(name)
                && ! limestone::internal::locator_format::is_locator_file_name(name)) {
                result.insert(name);
            }
        }
        return result;
    }

    // Restarts as a master, and reads all the entries (keys and values) the snapshot returns
    [[nodiscard]] std::vector<std::tuple<std::string, std::string>> restart_as_master_and_read() {
        limestone::api::configuration conf{};
        conf.set_data_location(location);
        datastore_ = std::make_unique<limestone::api::datastore_test>(conf);
        datastore_->ready();
        std::vector<std::tuple<std::string, std::string>> result{};
        std::unique_ptr<limestone::api::snapshot> snapshot = datastore_->get_snapshot();
        std::unique_ptr<limestone::api::cursor> cursor = snapshot->get_cursor();
        while (cursor->next()) {
            std::string key{};
            std::string value{};
            cursor->key(key);
            cursor->value(value);
            result.emplace_back(key, value);
        }
        return result;
    }

    std::unique_ptr<limestone::api::datastore_test> datastore_{};
};

}  // namespace limestone::testing
