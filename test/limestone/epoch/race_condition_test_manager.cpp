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

 #include "race_condition_test_manager.h"
 #include "logging_helper.h"

namespace limestone::testing {

thread_local size_t race_condition_test_manager::thread_local_id = 0;

race_condition_test_manager::race_condition_test_manager(std::vector<std::pair<test_method, size_t>> test_methods)
    : test_methods_(std::move(test_methods)) {}

void race_condition_test_manager::set_random_seed(unsigned int seed) {
    random_engine_.seed(seed);
}

void race_condition_test_manager::run() {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t thread_id_counter = 100;
    for (const auto p : test_methods_) {
        auto method = p.first;
        size_t count = p.second;
        for (size_t i = 0; i < count; ++i) {
            size_t current_id = thread_id_counter++;
            threads_.emplace_back([this, method, current_id]() {
                thread_local_id = current_id;
                VLOG_LP(50) << "Thread " << thread_local_id << " started.";
                try {
                    method();
                    thread_completed(current_id);
                } catch (const std::exception& e) {
                    std::lock_guard<std::mutex> lock(mutex_);
                    std::cerr << "Exception in thread: " << e.what() << std::endl;
                    thread_completed(current_id);
                } catch (...) {
                    std::lock_guard<std::mutex> lock(mutex_);
                    std::cerr << "Unknown exception in thread." << std::endl;
                    thread_completed(current_id);
                }
            });
        }
    }
}

void race_condition_test_manager::wait_at_hook(const std::string& hook_name) {
    size_t tid = thread_local_id; // Use the already-initialized thread_local_id
    {
        VLOG_LP(50) << "Thread " << tid << " waiting at hook: " << hook_name;
        std::unique_lock<std::mutex> lock(mutex_);
        pending_threads_.emplace(tid, hook_name);
        cv_.notify_all();
    }

    {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this, tid]() { return resumed_threads_.count(tid) > 0; });
        resumed_threads_.erase(tid);
        VLOG_LP(50) << "Thread " << tid << " resumed from hook: " << hook_name;
    }
}



void race_condition_test_manager::resume_one_thread() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (pending_threads_.empty()) return;

    std::uniform_int_distribution<size_t> dist(0, pending_threads_.size() - 1);
    auto it = pending_threads_.begin();
    std::advance(it, dist(random_engine_));
    resumed_threads_.insert(it->first);
    pending_threads_.erase(it);
    cv_.notify_all();
}

void race_condition_test_manager::generate_and_set_random_seed() {
    std::random_device rd;
    unsigned int seed = rd();
    std::cout << "Generated random seed: " << seed << std::endl;
    set_random_seed(seed);
}

void race_condition_test_manager::wait_for_all_threads_to_pause_or_complete() {
    VLOG_LP(50) << "Waiting for all threads to pause or complete.";
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this]() {
        return (pending_threads_.size() + threads_completed_ == threads_.size());
    });
}

bool race_condition_test_manager::all_threads_completed() {
    std::lock_guard<std::mutex> lock(mutex_);
    return threads_completed_ == threads_.size();
}

void race_condition_test_manager::join_all_threads() {
    for (auto& thread : threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void race_condition_test_manager::thread_completed(size_t thread_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    threads_completed_++;
    cv_.notify_all();
}

} // namespace limestone::testing