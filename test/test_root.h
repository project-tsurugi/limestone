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
    explicit datastore_test(configuration& conf) : datastore(conf) {}
    datastore_test() : datastore() {}

    // Provides access to internal members for testing purposes
    auto& log_channels() const noexcept { return log_channels_for_tests(); }
    auto epoch_id_informed() const noexcept { return epoch_id_informed_for_tests(); }
    auto epoch_id_recorded() const noexcept { return epoch_id_recorded_for_tests(); }
    auto epoch_id_switched() const noexcept { return epoch_id_switched_for_tests(); }
    auto& files() const noexcept { return files_for_tests(); }
    void rotate_epoch_file() { rotate_epoch_file_for_tests(); }

protected:
    // Overrides for on_wait1 to on_wait4 hooks to enable custom behavior during testing.
    void on_wait1() override {
        if (on_wait1_callback) on_wait1_callback();  // Executes the registered callback if set
    }
    void on_wait2() override {
        if (on_wait2_callback) on_wait2_callback();
    }
    void on_wait3() override {
        if (on_wait3_callback) on_wait3_callback();
    }
    void on_wait4() override {
        if (on_wait4_callback) on_wait4_callback();
    }

public:
    // Callback functions for testing on_wait1 to on_wait4 behavior.
    // These can be dynamically assigned in each test case.
    std::function<void()> on_wait1_callback;
    std::function<void()> on_wait2_callback;
    std::function<void()> on_wait3_callback;
    std::function<void()> on_wait4_callback;
};

} // namespace limestone::api
