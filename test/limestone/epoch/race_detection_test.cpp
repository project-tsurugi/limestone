/*
 * Copyright 2022-2025 Project Tsurugi.
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

#include <thread>
#include <sys/stat.h>
#include <iostream>
#include <vector>
#include <mutex>
#include <optional>
#include <boost/filesystem.hpp>

#include <limestone/logging.h>
#include <limestone/api/datastore.h>

#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"
#include "online_compaction.h"
#include "compaction_catalog.h"
#include "test_root.h"

using namespace limestone::api;
using namespace limestone::internal;


namespace limestone::testing {

class my_datastore : public datastore_test {
public:
    explicit my_datastore(const configuration& conf) : datastore_test(conf) {
        // Set the write_epoch_callback_ to track written epochs
        write_epoch_callback_ = [this](epoch_id_type epoch) {
            this->record_written_epoch(epoch);
        };

        // Set the persistent_callback_ to track persisted epochs
        add_persistent_callback([this](epoch_id_type epoch) {
            this->record_persisted_epoch(epoch);
        });
    }

    /**
     * @brief Get all epochs that were successfully written.
     * @return A vector containing all written epochs.
     */
    std::vector<epoch_id_type> get_written_epochs() const {
        std::lock_guard<std::mutex> lock(mutex_written_);
        return written_epochs_;
    }

    /**
     * @brief Get the first written epoch.
     * @return The first written epoch if available, otherwise std::nullopt.
     */
    std::optional<epoch_id_type> get_first_written_epoch() const {
        std::lock_guard<std::mutex> lock(mutex_written_);
        if (!written_epochs_.empty()) {
            return written_epochs_.front();
        }
        return std::nullopt;
    }

    /**
     * @brief Get the last written epoch.
     * @return The last written epoch if available, otherwise std::nullopt.
     */
    std::optional<epoch_id_type> get_last_written_epoch() const {
        std::lock_guard<std::mutex> lock(mutex_written_);
        if (!written_epochs_.empty()) {
            return written_epochs_.back();
        }
        return std::nullopt;
    }

    /**
     * @brief Get the number of written epochs.
     * @return The size of the written_epochs_ list.
     */
    std::size_t get_written_epoch_count() const {
        std::lock_guard<std::mutex> lock(mutex_written_);
        return written_epochs_.size();
    }

    /**
     * @brief Get all epochs that were successfully persisted.
     * @return A vector containing all persisted epochs.
     */
    std::vector<epoch_id_type> get_persisted_epochs() const {
        std::lock_guard<std::mutex> lock(mutex_persisted_);
        return persisted_epochs_;
    }

    /**
     * @brief Get the first persisted epoch.
     * @return The first persisted epoch if available, otherwise std::nullopt.
     */
    std::optional<epoch_id_type> get_first_persisted_epoch() const {
        std::lock_guard<std::mutex> lock(mutex_persisted_);
        if (!persisted_epochs_.empty()) {
            return persisted_epochs_.front();
        }
        return std::nullopt;
    }

    /**
     * @brief Get the last persisted epoch.
     * @return The last persisted epoch if available, otherwise std::nullopt.
     */
    std::optional<epoch_id_type> get_last_persisted_epoch() const {
        std::lock_guard<std::mutex> lock(mutex_persisted_);
        if (!persisted_epochs_.empty()) {
            return persisted_epochs_.back();
        }
        return std::nullopt;
    }

    /**
     * @brief Get the number of persisted epochs.
     * @return The size of the persisted_epochs_ list.
     */
    std::size_t get_persisted_epoch_count() const {
        std::lock_guard<std::mutex> lock(mutex_persisted_);
        return persisted_epochs_.size();
    }

private:
    void record_written_epoch(epoch_id_type epoch) {
        std::lock_guard<std::mutex> lock(mutex_written_);
        written_epochs_.emplace_back(epoch);
    }

    void record_persisted_epoch(epoch_id_type epoch) {
        std::lock_guard<std::mutex> lock(mutex_persisted_);
        persisted_epochs_.emplace_back(epoch);
    }

    mutable std::mutex mutex_written_;  // Protects access to the written_epochs_ list
    mutable std::mutex mutex_persisted_;  // Protects access to the persisted_epochs_ list
    std::vector<epoch_id_type> written_epochs_;  // Stores successfully written epochs
    std::vector<epoch_id_type> persisted_epochs_;  // Stores successfully persisted epochs
};

class race_detection_test : public ::testing::Test {
public:
    static constexpr const char* location = "/tmp/race_detection_test";

    void SetUp() {
        if (boost::filesystem::exists(location)) {
            boost::filesystem::permissions(location, boost::filesystem::owner_all);
        }
        boost::filesystem::remove_all(location);
        if (!boost::filesystem::create_directory(location)) {
            std::cerr << "cannot make directory" << std::endl;
        }
        gen_datastore();
        datastore_->ready();
        datastore_->switch_epoch(1);
    }

    void gen_datastore() {
        std::vector<boost::filesystem::path> data_locations{};
        data_locations.emplace_back(location);
        boost::filesystem::path metadata_location{location};
        limestone::api::configuration conf(data_locations, metadata_location);

        datastore_ = std::make_unique<my_datastore>(conf);
        lc0_ = &datastore_->create_channel(location);
        lc1_ = &datastore_->create_channel(location);
    }

    void TearDown() {
        datastore_->shutdown();
        datastore_ = nullptr;
        if (boost::filesystem::exists(location)) {
            boost::filesystem::permissions(location, boost::filesystem::owner_all);
        }
        boost::filesystem::remove_all(location);
    }

protected:
    std::unique_ptr<my_datastore> datastore_{};
    log_channel* lc0_{};
    log_channel* lc1_{};
    std::atomic_uint64_t epoch_id_{1};

    void switch_epoch() {
        epoch_id_.fetch_add(1);
        datastore_->switch_epoch(epoch_id_.load());
    }

    void write_to_log_channel0() {
        lc0_->begin_session();
        lc0_->end_session();
    }

    void write_to_log_channel1() {
        lc1_->begin_session();
        lc1_->end_session();
    }

};


TEST_F(race_detection_test, race_detection_behavior_test) {
    EXPECT_EQ(lc0_->current_epoch_id(), UINT64_MAX);
    EXPECT_EQ(lc0_->finished_epoch_id(), 0);
    EXPECT_EQ(lc1_->current_epoch_id(), UINT64_MAX);
    EXPECT_EQ(lc1_->finished_epoch_id(), 0);
    EXPECT_EQ(datastore_->epoch_id_informed(), 0);
    EXPECT_EQ(datastore_->epoch_id_to_be_recorded(), 0);
    EXPECT_EQ(datastore_->epoch_id_record_finished(), 0);
    EXPECT_EQ(datastore_->epoch_id_switched(), 1);
    EXPECT_EQ(datastore_->get_written_epoch_count(), 0);
    EXPECT_EQ(datastore_->get_persisted_epoch_count(), 0);
    switch_epoch();
    EXPECT_EQ(lc0_->current_epoch_id(), UINT64_MAX);
    EXPECT_EQ(lc0_->finished_epoch_id(), 0);
    EXPECT_EQ(lc1_->current_epoch_id(), UINT64_MAX);
    EXPECT_EQ(lc1_->finished_epoch_id(), 0);
    EXPECT_EQ(datastore_->epoch_id_informed(), 1);
    EXPECT_EQ(datastore_->epoch_id_to_be_recorded(), 0);
    EXPECT_EQ(datastore_->epoch_id_record_finished(), 0);
    EXPECT_EQ(datastore_->epoch_id_switched(), 2);
    EXPECT_EQ(datastore_->get_written_epoch_count(), 0);
    EXPECT_EQ(datastore_->get_persisted_epoch_count(), 1);
    EXPECT_EQ(datastore_->get_last_persisted_epoch(), 1);
    switch_epoch();
    EXPECT_EQ(lc0_->current_epoch_id(), UINT64_MAX);
    EXPECT_EQ(lc0_->finished_epoch_id(), 0);
    EXPECT_EQ(lc1_->current_epoch_id(), UINT64_MAX);
    EXPECT_EQ(lc1_->finished_epoch_id(), 0);
    EXPECT_EQ(datastore_->epoch_id_informed(), 2);
    EXPECT_EQ(datastore_->epoch_id_to_be_recorded(), 0);
    EXPECT_EQ(datastore_->epoch_id_record_finished(), 0);
    EXPECT_EQ(datastore_->epoch_id_switched(), 3);
    EXPECT_EQ(datastore_->get_written_epoch_count(), 0);
    EXPECT_EQ(datastore_->get_persisted_epoch_count(), 2);
    EXPECT_EQ(datastore_->get_last_persisted_epoch(), 2);
    write_to_log_channel0();
    EXPECT_EQ(lc0_->current_epoch_id(), UINT64_MAX);
    EXPECT_EQ(lc0_->finished_epoch_id(), 3);
    EXPECT_EQ(lc1_->current_epoch_id(), UINT64_MAX);
    EXPECT_EQ(lc1_->finished_epoch_id(), 0);
    EXPECT_EQ(datastore_->epoch_id_informed(), 2);
    EXPECT_EQ(datastore_->epoch_id_to_be_recorded(), 2);
    EXPECT_EQ(datastore_->epoch_id_record_finished(), 2);
    EXPECT_EQ(datastore_->epoch_id_switched(), 3);
    EXPECT_EQ(datastore_->get_written_epoch_count(), 1);
    EXPECT_EQ(datastore_->get_last_written_epoch(), 2);
    EXPECT_EQ(datastore_->get_persisted_epoch_count(), 2);
    EXPECT_EQ(datastore_->get_last_persisted_epoch(), 2);
    switch_epoch();
    EXPECT_EQ(lc0_->current_epoch_id(), UINT64_MAX);
    EXPECT_EQ(lc0_->finished_epoch_id(), 3);
    EXPECT_EQ(lc1_->current_epoch_id(), UINT64_MAX);
    EXPECT_EQ(lc1_->finished_epoch_id(), 0);
    EXPECT_EQ(datastore_->epoch_id_informed(), 3);
    EXPECT_EQ(datastore_->epoch_id_to_be_recorded(), 3);
    EXPECT_EQ(datastore_->epoch_id_record_finished(), 3);
    EXPECT_EQ(datastore_->epoch_id_switched(), 4);
    EXPECT_EQ(datastore_->get_written_epoch_count(), 2);
    EXPECT_EQ(datastore_->get_last_written_epoch(), 3);
    EXPECT_EQ(datastore_->get_persisted_epoch_count(), 3);
    EXPECT_EQ(datastore_->get_last_persisted_epoch(), 3);
    write_to_log_channel1();
    EXPECT_EQ(lc0_->current_epoch_id(), UINT64_MAX);
    EXPECT_EQ(lc0_->finished_epoch_id(), 3);
    EXPECT_EQ(lc1_->current_epoch_id(), UINT64_MAX);
    EXPECT_EQ(lc1_->finished_epoch_id(), 4);
    EXPECT_EQ(datastore_->epoch_id_informed(), 3);
    EXPECT_EQ(datastore_->epoch_id_to_be_recorded(), 3);
    EXPECT_EQ(datastore_->epoch_id_record_finished(), 3);
    EXPECT_EQ(datastore_->epoch_id_switched(), 4);
    EXPECT_EQ(datastore_->get_written_epoch_count(), 2);
    EXPECT_EQ(datastore_->get_last_written_epoch(), 3);
    EXPECT_EQ(datastore_->get_persisted_epoch_count(), 3);
    EXPECT_EQ(datastore_->get_last_persisted_epoch(), 3);
    switch_epoch();
    EXPECT_EQ(lc0_->current_epoch_id(), UINT64_MAX);
    EXPECT_EQ(lc0_->finished_epoch_id(), 3);
    EXPECT_EQ(lc1_->current_epoch_id(), UINT64_MAX);
    EXPECT_EQ(lc1_->finished_epoch_id(), 4);
    EXPECT_EQ(datastore_->epoch_id_informed(), 4);
    EXPECT_EQ(datastore_->epoch_id_to_be_recorded(), 4);
    EXPECT_EQ(datastore_->epoch_id_record_finished(), 4);
    EXPECT_EQ(datastore_->epoch_id_switched(), 5);
    EXPECT_EQ(datastore_->get_written_epoch_count(), 3);
    EXPECT_EQ(datastore_->get_last_written_epoch(), 4);
    EXPECT_EQ(datastore_->get_persisted_epoch_count(), 4);
    EXPECT_EQ(datastore_->get_last_persisted_epoch(), 4);
}


} // namespace limestone::testing


