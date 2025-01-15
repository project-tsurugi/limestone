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
#include <random>
#include <optional>
#include <boost/filesystem.hpp>

#include <limestone/logging.h>
#include <limestone/api/datastore.h>
#include "race_condition_test_manager.h"

#include "dblog_scan.h"
#include "internal.h"
#include "log_entry.h"
#include "online_compaction.h"
#include "compaction_catalog.h"
#include "test_root.h"

using namespace limestone::api;
using namespace limestone::internal;


namespace limestone::testing {

static constexpr const char* OPERATION_WRITE_EPOCH = "write_epoch";
static constexpr const char* OPERATION_PERSIST_CALLBACK = "persist_callback";

// General utility for setting callbacks
template <typename Hook>
void set_callback(std::shared_ptr<limestone::testing::race_condition_test_manager> manager, 
                  Hook& callback, const char* hook_name) {
    callback = [manager, hook_name]() {
        manager->wait_at_hook(hook_name);
    };
}
class my_datastore : public datastore_test {
public:
    explicit my_datastore(const configuration& conf) : datastore_test(conf) {
        // Set the write_epoch_callback_ to track written epochs
        set_write_epoch_callback([this](epoch_id_type epoch) {
            this->record_written_epoch(epoch);
            this->log_operation(OPERATION_WRITE_EPOCH, epoch);
        });

        // Set the persistent_callback_ to track persisted epochs
        add_persistent_callback([this](epoch_id_type epoch) {
            this->record_persisted_epoch(epoch);
            this->log_operation(OPERATION_PERSIST_CALLBACK, epoch);
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

    /**
     * @brief Get the operation log for written and persisted epochs.
     * @return A vector containing operation logs in the form of <operation, epoch>.
     */
    std::vector<std::pair<std::string, epoch_id_type>> get_operation_log() const {
        std::lock_guard<std::mutex> lock(mutex_log_);
        return operation_log_;
    }

    /**
     * @brief Print the operation log for debugging.
     */
    void print_operation_log() const {
        auto operation_log = get_operation_log();
        std::cerr << "Operation log contents:" << std::endl;
        for (const auto& [operation, epoch] : operation_log) {
            std::cerr << "Operation: " << operation << ", Epoch: " << epoch << std::endl;
        }
    }

    /**
     * @brief Registers a `race_condition_test_manager` instance for hook-based synchronization.
     * 
     * This method should be called during the initialization of the `my_datastore` instance.
     * Since it is expected to be called in a single-threaded context during initialization,
     * thread safety is not required.
     *
     * @param manager The `race_condition_test_manager` instance to register.
     */
    void register_race_condition_manager(
        std::shared_ptr<limestone::testing::race_condition_test_manager> manager) {
        race_condition_manager_ = manager;

        set_callback(manager, on_begin_session_current_epoch_id_store_callback, "on_begin_session_current_epoch_id_store");
        set_callback(manager, on_end_session_finished_epoch_id_store_callback, "on_end_session_finished_epoch_id_store");
        set_callback(manager, on_end_session_current_epoch_id_store_callback, "on_end_session_current_epoch_id_store");
        set_callback(manager, on_switch_epoch_epoch_id_switched_store_callback, "on_switch_epoch_epoch_id_switched_store");
        set_callback(manager, on_update_min_epoch_id_epoch_id_switched_load_callback, "on_update_min_epoch_id_epoch_id_switched_load");
        set_callback(manager, on_update_min_epoch_id_current_epoch_id_load_callback, "on_update_min_epoch_id_current_epoch_id_load");
        set_callback(manager, on_update_min_epoch_id_finished_epoch_id_load_callback, "on_update_min_epoch_id_finished_epoch_id_load");
        set_callback(manager, on_update_min_epoch_id_epoch_id_to_be_recorded_load_callback, "on_update_min_epoch_id_epoch_id_to_be_recorded_load");
        set_callback(manager, on_update_min_epoch_id_epoch_id_to_be_recorded_cas_callback, "on_update_min_epoch_id_epoch_id_to_be_recorded_cas");
        set_callback(manager, on_update_min_epoch_id_epoch_id_record_finished_load_callback, "on_update_min_epoch_id_epoch_id_record_finished_load");
        set_callback(manager, on_update_min_epoch_id_epoch_id_informed_load_1_callback, "on_update_min_epoch_id_epoch_id_informed_load_1");
        set_callback(manager, on_update_min_epoch_id_epoch_id_informed_cas_callback, "on_update_min_epoch_id_epoch_id_informed_cas");
        set_callback(manager, on_update_min_epoch_id_epoch_id_informed_load_2_callback, "on_update_min_epoch_id_epoch_id_informed_load_2");
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

    void log_operation(const std::string& operation, epoch_id_type epoch) {
        std::lock_guard<std::mutex> lock(mutex_log_);
        operation_log_.emplace_back(operation, epoch);
    }

    mutable std::mutex mutex_written_;  // Protects access to the written_epochs_ list
    mutable std::mutex mutex_persisted_;  // Protects access to the persisted_epochs_ list
    mutable std::mutex mutex_log_;  // Protects access to the operation log

    std::vector<epoch_id_type> written_epochs_;  // Stores successfully written epochs
    std::vector<epoch_id_type> persisted_epochs_;  // Stores successfully persisted epochs
    std::vector<std::pair<std::string, epoch_id_type>> operation_log_;  // Logs operations and epochs

    /**
     * @brief Invokes a hook in the registered race condition test manager, if available.
     * @param hook_name The name of the hook to invoke.
     */
    void invoke_hook(const std::string& hook_name) {
        if (race_condition_manager_) {
            race_condition_manager_->wait_at_hook(hook_name);
        }
    }

    std::shared_ptr<limestone::testing::race_condition_test_manager> race_condition_manager_;
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


TEST_F(race_detection_test, example) {
    // TestManager の初期化
    auto manager = std::make_shared<race_condition_test_manager>(std::vector<std::pair<std::function<void()>, size_t>>{
        { [this]() { switch_epoch(); }, 1 },
        { [this]() { write_to_log_channel0(); }, 1 },
        { [this]() { write_to_log_channel1(); }, 1 }
    });

    datastore_->register_race_condition_manager(manager);
    FLAGS_v = 50;

    manager->run();

    // 全てのスレッドが待機または終了するまで待つ
    manager->wait_for_all_threads_to_pause_or_complete();

    // スレッドを順番に再開
    while (!manager->all_threads_completed()) {
        manager->resume_one_thread();
        manager->wait_for_all_threads_to_pause_or_complete();
    }

    manager->join_all_threads();
}



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
    EXPECT_EQ(datastore_->epoch_id_to_be_recorded(), 0);
    EXPECT_EQ(datastore_->epoch_id_record_finished(), 0);
    EXPECT_EQ(datastore_->epoch_id_switched(), 3);
    EXPECT_EQ(datastore_->get_written_epoch_count(), 0);
    EXPECT_EQ(datastore_->get_persisted_epoch_count(), 2);
    EXPECT_EQ(datastore_->get_last_persisted_epoch(), 2);
    switch_epoch();
    datastore_->print_operation_log();
    EXPECT_EQ(lc0_->current_epoch_id(), UINT64_MAX);
    EXPECT_EQ(lc0_->finished_epoch_id(), 3);
    EXPECT_EQ(lc1_->current_epoch_id(), UINT64_MAX);
    EXPECT_EQ(lc1_->finished_epoch_id(), 0);
    EXPECT_EQ(datastore_->epoch_id_informed(), 3);
    EXPECT_EQ(datastore_->epoch_id_to_be_recorded(), 3);
    EXPECT_EQ(datastore_->epoch_id_record_finished(), 3);
    EXPECT_EQ(datastore_->epoch_id_switched(), 4);
    EXPECT_EQ(datastore_->get_written_epoch_count(), 1);
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
    EXPECT_EQ(datastore_->get_written_epoch_count(), 1);
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
    EXPECT_EQ(datastore_->get_written_epoch_count(), 2);
    EXPECT_EQ(datastore_->get_last_written_epoch(), 4);
    EXPECT_EQ(datastore_->get_persisted_epoch_count(), 4);
    EXPECT_EQ(datastore_->get_last_persisted_epoch(), 4);

    // Verify operation log contents
    auto operation_log = datastore_->get_operation_log();
    std::vector<std::pair<std::string, epoch_id_type>> expected_log = {
        {OPERATION_PERSIST_CALLBACK, 1},
        {OPERATION_PERSIST_CALLBACK, 2},
        {OPERATION_WRITE_EPOCH, 3},
        {OPERATION_PERSIST_CALLBACK, 3},
        {OPERATION_WRITE_EPOCH, 4},
        {OPERATION_PERSIST_CALLBACK, 4},
    };

    ASSERT_EQ(operation_log.size(), expected_log.size()) << "Operation log size mismatch.";

    for (size_t i = 0; i < operation_log.size(); ++i) {
        EXPECT_EQ(operation_log[i].first, expected_log[i].first) << "Mismatch at log index " << i << " for operation.";
        EXPECT_EQ(operation_log[i].second, expected_log[i].second) << "Mismatch at log index " << i << " for epoch ID.";
    }

    // Print operation log for debugging
    datastore_->print_operation_log();
}




} // namespace limestone::testing


