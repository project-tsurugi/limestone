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

#ifndef RACE_CONDITION_TEST_MANAGER_H
#define RACE_CONDITION_TEST_MANAGER_H

#include <vector>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <random>
#include <functional>
#include <iostream>

namespace limestone::testing {

class race_condition_test_manager {
public:
    using test_method = std::function<void()>;

    explicit race_condition_test_manager(std::vector<std::pair<test_method, size_t>> test_methods);

    void set_random_seed(unsigned int seed);
    void run();
    void wait_at_hook(const std::string& hook_name);
    void resume_one_thread();
    void generate_and_set_random_seed();
    void wait_for_all_threads_to_pause_or_complete();
    bool all_threads_completed();
    void join_all_threads();

private:
    std::vector<std::pair<test_method, size_t>> test_methods_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<std::thread> threads_;
    std::map<size_t, std::string> pending_threads_;
    std::set<size_t> resumed_threads_;
    size_t threads_completed_ = 0;
    std::mt19937 random_engine_{std::random_device{}()};

    void thread_completed(size_t thread_id);
};

} // namespace limestone::testing

#endif // RACE_CONDITION_TEST_MANAGER_H
