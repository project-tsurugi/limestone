/*
 * Copyright 2019-2024 Project Tsurugi.
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

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <limestone/api/datastore.h>
#include <limestone/api/configuration.h>

namespace limestone::api {

class datastore_test : public datastore {
public:
    explicit datastore_test(const configuration& conf) : datastore(conf) {}
    datastore_test() : datastore() {}

    // Provides access to internal members for testing purposes
    auto& log_channels() const noexcept { return log_channels_for_tests(); }
    auto epoch_id_informed() const noexcept { return epoch_id_informed_for_tests(); }
    auto epoch_id_to_be_recorded() const noexcept { return epoch_id_to_be_recorded_for_tests(); }
    auto epoch_id_record_finished() const noexcept { return epoch_id_record_finished_for_tests(); }
    auto epoch_id_switched() const noexcept { return epoch_id_switched_for_tests(); }
    auto& files() const noexcept { return files_for_tests(); }
    void rotate_epoch_file() { rotate_epoch_file_for_tests(); }

protected:
    inline void execute_callback(const std::function<void()>& callback) noexcept {
        if (callback) {
            callback();
        }
    }

    // Overrides for hook methods in the datastore class.
    // These hooks provide customizable behavior for testing various operations, including
    // epoch management, log rotation, and session handling.
    void on_rotate_log_files() noexcept override {
        execute_callback(on_rotate_log_files_callback);
    }
    void on_begin_session_current_epoch_id_store() noexcept override {
        execute_callback(on_begin_session_current_epoch_id_store_callback);
    }
    void on_end_session_finished_epoch_id_store() noexcept override {
        execute_callback(on_end_session_finished_epoch_id_store_callback);
    }
    void on_end_session_current_epoch_id_store() noexcept override {
        execute_callback(on_end_session_current_epoch_id_store_callback);
    }
    void on_switch_epoch_epoch_id_switched_store() noexcept override {
        execute_callback(on_switch_epoch_epoch_id_switched_store_callback);
    }
    void on_update_min_epoch_id_epoch_id_switched_load() noexcept override {
        execute_callback(on_update_min_epoch_id_epoch_id_switched_load_callback);
    }
    void on_update_min_epoch_id_current_epoch_id_load() noexcept override {
        execute_callback(on_update_min_epoch_id_current_epoch_id_load_callback);
    }
    void on_update_min_epoch_id_finished_epoch_id_load() noexcept override {
        execute_callback(on_update_min_epoch_id_finished_epoch_id_load_callback);
    }
    void on_update_min_epoch_id_epoch_id_to_be_recorded_load() noexcept override {
        execute_callback(on_update_min_epoch_id_epoch_id_to_be_recorded_load_callback);
    }
    void on_update_min_epoch_id_epoch_id_to_be_recorded_cas() noexcept override {
        execute_callback(on_update_min_epoch_id_epoch_id_to_be_recorded_cas_callback);
    }
    void on_update_min_epoch_id_epoch_id_record_finished_load() noexcept override {
        execute_callback(on_update_min_epoch_id_epoch_id_record_finished_load_callback);
    }
    void on_update_min_epoch_id_epoch_id_informed_load_1() noexcept override {
        execute_callback(on_update_min_epoch_id_epoch_id_informed_load_1_callback);
    }
    void on_update_min_epoch_id_epoch_id_informed_cas() noexcept override {
        execute_callback(on_update_min_epoch_id_epoch_id_informed_cas_callback);
    }
    void on_update_min_epoch_id_epoch_id_informed_load_2() noexcept override {
        execute_callback(on_update_min_epoch_id_epoch_id_informed_load_2_callback);
    }


public:
    // Callback functions for testing various hooks in the datastore class.
    // These functions can be dynamically assigned to customize behavior during testing of
    // operations such as epoch management, log rotation, and session handling.
    //
    // Example usage:
    // test_instance.on_end_session_finished_epoch_id_store_callback = []() {
    //     std::cout << "Callback triggered for on_end_session_finished_epoch_id_store" << std::endl;
    // };
    std::function<void()> on_rotate_log_files_callback;
    std::function<void()> on_begin_session_current_epoch_id_store_callback;
    std::function<void()> on_end_session_finished_epoch_id_store_callback;
    std::function<void()> on_end_session_current_epoch_id_store_callback;
    std::function<void()> on_switch_epoch_epoch_id_switched_store_callback;
    std::function<void()> on_update_min_epoch_id_epoch_id_switched_load_callback;
    std::function<void()> on_update_min_epoch_id_current_epoch_id_load_callback;
    std::function<void()> on_update_min_epoch_id_finished_epoch_id_load_callback;
    std::function<void()> on_update_min_epoch_id_epoch_id_to_be_recorded_load_callback;
    std::function<void()> on_update_min_epoch_id_epoch_id_to_be_recorded_cas_callback;
    std::function<void()> on_update_min_epoch_id_epoch_id_record_finished_load_callback;
    std::function<void()> on_update_min_epoch_id_epoch_id_informed_load_1_callback;
    std::function<void()> on_update_min_epoch_id_epoch_id_informed_cas_callback;
    std::function<void()> on_update_min_epoch_id_epoch_id_informed_load_2_callback;
};

} // namespace limestone::api
