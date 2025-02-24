#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <cstdlib> 
#include "test_root.h"
#include "log_entry.h"
#include "blob_file_garbage_collector.h"
#include "limestone/logging.h"
#include "blob_file_resolver.h"

namespace limestone::testing {

using namespace limestone::internal;

class blob_file_gc_state_machine_test : public ::testing::Test {
    protected:
        blob_file_gc_state_machine state_machine_;
    
        void SetUp() override {
            state_machine_.force_set_state(blob_file_gc_state::not_started);
        }
    
        /**
         * @brief Verifies that a valid state transition occurs.
         */
        void verify_transition(blob_file_gc_state initial_state, blob_file_gc_event event, bool should_throw) {
            state_machine_.force_set_state(initial_state);
    
            if (should_throw) {
                EXPECT_THROW(state_machine_.transition(event), std::logic_error)
                    << "Unexpectedly allowed transition from " 
                    << blob_file_gc_state_machine::to_string(initial_state) 
                    << " with event " << static_cast<int>(event);
            } else {
                EXPECT_NO_THROW({
                    blob_file_gc_state new_state = state_machine_.transition(event);
                    EXPECT_NE(new_state, initial_state) 
                        << "Transition did not change state: " 
                        << blob_file_gc_state_machine::to_string(initial_state) 
                        << " -> " << blob_file_gc_state_machine::to_string(new_state);
                });
            }
        }
    };
    
    TEST_F(blob_file_gc_state_machine_test, test_all_state_transitions) {
        for (int s = static_cast<int>(blob_file_gc_state::not_started);
             s <= static_cast<int>(blob_file_gc_state::shutdown); ++s) {
            
            blob_file_gc_state current_state = static_cast<blob_file_gc_state>(s);
            
    
            for (int e = static_cast<int>(blob_file_gc_event::start_blob_scan);
                 e <= static_cast<int>(blob_file_gc_event::reset); ++e) {
    
                blob_file_gc_event event = static_cast<blob_file_gc_event>(e);
                auto expected_next_state = state_machine_.get_next_state_if_valid(current_state, event);
    
                state_machine_.force_set_state(current_state);
                std::cerr << "[TEST] State: " << blob_file_gc_state_machine::to_string(current_state)
                << ", Event: " << blob_file_gc_state_machine::to_string(event)
                << " -> Expected: "
                << (expected_next_state ? blob_file_gc_state_machine::to_string(*expected_next_state) : "Exception (std::logic_error)")
                << std::endl;
    
                if (expected_next_state) {
                    blob_file_gc_state new_state;
                    state_machine_.force_set_state(current_state);
                    EXPECT_NO_THROW(new_state = state_machine_.transition(event))
                        << "Valid transition failed: " 
                        << blob_file_gc_state_machine::to_string(current_state)
                        << " -> " << blob_file_gc_state_machine::to_string(*expected_next_state);
    
                    EXPECT_EQ(new_state, *expected_next_state)
                        << "Transition result does not match expected state: " 
                        << blob_file_gc_state_machine::to_string(current_state)
                        << " -> " << blob_file_gc_state_machine::to_string(new_state);
                } else {
                    EXPECT_THROW(state_machine_.transition(event), std::logic_error)
                        << "Invalid transition did not throw: " 
                        << blob_file_gc_state_machine::to_string(current_state)
                        << " -> " << blob_file_gc_state_machine::to_string(event);
                }
            }
        }
    }
    
    
    /**
     * @brief Test: Shutdown should always be allowed.
     */
    TEST_F(blob_file_gc_state_machine_test, shutdown_always_allowed) {
        for (int s = static_cast<int>(blob_file_gc_state::not_started);
                s <= static_cast<int>(blob_file_gc_state::completed); ++s) {
    
            blob_file_gc_state current_state = static_cast<blob_file_gc_state>(s);
            state_machine_.force_set_state(current_state);
    
            EXPECT_NO_THROW(state_machine_.transition(blob_file_gc_event::shutdown))
                << "Shutdown should always be allowed from " 
                << blob_file_gc_state_machine::to_string(current_state);
        }
    }
        
    TEST_F(blob_file_gc_state_machine_test, concurrent_start_blob_scan) {
        // Number of threads attempting to transition
        const int thread_count = 10;
        std::vector<std::thread> threads;
        std::atomic<int> success_count{0};
        std::atomic<int> failure_count{0};
    
        for (int i = 0; i < thread_count; ++i) {
            threads.emplace_back([&]() {
                try {
                    state_machine_.start_blob_scan();
                    success_count++;
                } catch (const std::logic_error&) {
                    failure_count++;
                }
            });
        }
    
        for (auto& thread : threads) {
            thread.join();
        }
    
        // Ensure that exactly one thread successfully starts the scan
        EXPECT_EQ(success_count.load(), 1);
        // Ensure that all other threads encountered an error
        EXPECT_EQ(failure_count.load(), thread_count - 1);
    }      
    
    TEST_F(blob_file_gc_state_machine_test, concurrent_start_snapshot_scan) {
        const int thread_count = 10;
        std::vector<std::thread> threads;
        std::atomic<int> success_count{0};
        std::atomic<int> failure_count{0};
    
        for (int i = 0; i < thread_count; ++i) {
            threads.emplace_back([&]() {
                try {
                    state_machine_.start_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal);
                    success_count++;
                } catch (const std::logic_error&) {
                    failure_count++;
                }
            });
        }
    
        for (auto& thread : threads) {
            thread.join();
        }
    
        EXPECT_EQ(success_count.load(), 1);
        EXPECT_EQ(failure_count.load(), thread_count - 1);
    }
    
    TEST_F(blob_file_gc_state_machine_test, concurrent_complete_blob_scan) {
        state_machine_.start_blob_scan();
    
        const int thread_count = 10;
        std::vector<std::thread> threads;
        std::atomic<int> success_count{0};
        std::atomic<int> failure_count{0};
    
        for (int i = 0; i < thread_count; ++i) {
            threads.emplace_back([&]() {
                try {
                    state_machine_.complete_blob_scan();
                    success_count++;
                } catch (const std::logic_error&) {
                    failure_count++;
                }
            });
        }
    
        for (auto& thread : threads) {
            thread.join();
        }
    
        EXPECT_EQ(success_count.load(), 1);
        EXPECT_EQ(failure_count.load(), thread_count - 1);
    }
    
    TEST_F(blob_file_gc_state_machine_test, concurrent_complete_snapshot_scan) {
        state_machine_.start_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal);
    
        const int thread_count = 10;
        std::vector<std::thread> threads;
        std::atomic<int> success_count{0};
        std::atomic<int> failure_count{0};
    
        for (int i = 0; i < thread_count; ++i) {
            threads.emplace_back([&]() {
                try {
                    state_machine_.complete_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal);
                    success_count++;
                } catch (const std::logic_error&) {
                    failure_count++;
                }
            });
        }
    
        for (auto& thread : threads) {
            thread.join();
        }
    
        EXPECT_EQ(success_count.load(), 1);
        EXPECT_EQ(failure_count.load(), thread_count - 1);
    }
    
    TEST_F(blob_file_gc_state_machine_test, concurrent_complete_cleanup) {
        state_machine_.force_set_state(blob_file_gc_state::cleaning_up);
    
        const int thread_count = 10;
        std::vector<std::thread> threads;
        std::atomic<int> success_count{0};
        std::atomic<int> failure_count{0};
    
        for (int i = 0; i < thread_count; ++i) {
            threads.emplace_back([&]() {
                try {
                    state_machine_.complete_cleanup();
                    success_count++;
                } catch (const std::logic_error&) {
                    failure_count++;
                }
            });
        }
    
        for (auto& thread : threads) {
            thread.join();
        }
    
        EXPECT_EQ(success_count.load(), 1);
        EXPECT_EQ(failure_count.load(), thread_count - 1);
    }
    
    TEST_F(blob_file_gc_state_machine_test, concurrent_shutdown) {
        state_machine_.force_set_state(blob_file_gc_state::scanning_both);
    
        constexpr int thread_count = 10;
        std::vector<std::thread> threads;
        std::atomic<int> success_count{0};
    
        for (int i = 0; i < thread_count; ++i) {
            threads.emplace_back([&]() {
                state_machine_.shutdown();
                success_count++;
            });
        }
    
        for (auto& thread : threads) {
            thread.join();
        }
    
        // shutdown() can be safely called from multiple threads.
        EXPECT_EQ(success_count.load(), thread_count);
        EXPECT_EQ(state_machine_.get_state(), blob_file_gc_state::shutdown);
    }
    
    // ================= blob_file_gc_state_machine Transition Tests =================
    
    void assert_transition(std::function<blob_file_gc_state()> transition_func, 
                           blob_file_gc_state_machine& state_machine,
                           blob_file_gc_state expected_state) {
        try {
            // Execute the transition function and obtain the actual next state
            blob_file_gc_state actual_state = transition_func();
            
            // Check the return value and the state machine's internal state
            EXPECT_EQ(actual_state, expected_state)
                << "Transition function returned incorrect state."
                << " Expected: " << blob_file_gc_state_machine::to_string(expected_state)
                << ", Actual: " << blob_file_gc_state_machine::to_string(actual_state);
    
            EXPECT_EQ(state_machine.get_state(), expected_state)
                << "State machine's internal state does not match expected state."
                << " Expected: " << blob_file_gc_state_machine::to_string(expected_state)
                << ", Actual: " << blob_file_gc_state_machine::to_string(state_machine.get_state());
    
            } catch (const std::exception& e) {
            // If an exception is thrown, report test failure and continue
            ADD_FAILURE() << "Exception thrown during state transition: " << e.what();
        }
    }
    
    
    /**
     * @brief Tests the transition where BLOB scan starts first, followed by snapshot scan.
     */
    /**
     * @brief Tests the transition where BLOB scan starts first, followed by snapshot scan.
     */
    TEST_F(blob_file_gc_state_machine_test, transition_blob_first) {
        assert_transition([&]() { return state_machine_.start_blob_scan(); }, state_machine_, blob_file_gc_state::scanning_blob_only);
        assert_transition([&]() { return state_machine_.start_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal); }, state_machine_, blob_file_gc_state::scanning_both);
        assert_transition([&]() { return state_machine_.complete_blob_scan(); }, state_machine_, blob_file_gc_state::blob_scan_completed_snapshot_in_progress);
        assert_transition([&]() { return state_machine_.complete_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal); }, state_machine_, blob_file_gc_state::cleaning_up);
        assert_transition([&]() { return state_machine_.complete_cleanup(); }, state_machine_, blob_file_gc_state::completed);
        assert_transition([&]() { return state_machine_.shutdown(); }, state_machine_, blob_file_gc_state::shutdown);
        assert_transition([&]() { return state_machine_.reset(); }, state_machine_, blob_file_gc_state::not_started);
    }
    
    /**
     * @brief Tests the transition where snapshot scan starts first, followed by BLOB scan.
     */
    TEST_F(blob_file_gc_state_machine_test, transition_snapshot_first) {
        assert_transition([&]() { return state_machine_.start_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal); }, state_machine_, blob_file_gc_state::scanning_snapshot_only);
        assert_transition([&]() { return state_machine_.start_blob_scan(); }, state_machine_, blob_file_gc_state::scanning_both);
        assert_transition([&]() { return state_machine_.complete_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal); }, state_machine_, blob_file_gc_state::snapshot_scan_completed_blob_in_progress);
        assert_transition([&]() { return state_machine_.complete_blob_scan(); }, state_machine_, blob_file_gc_state::cleaning_up);
        assert_transition([&]() { return state_machine_.complete_cleanup(); }, state_machine_, blob_file_gc_state::completed);
        assert_transition([&]() { return state_machine_.shutdown(); }, state_machine_, blob_file_gc_state::shutdown);
        assert_transition([&]() { return state_machine_.reset(); }, state_machine_, blob_file_gc_state::not_started);
    }
    
    /**
     * @brief Tests the transition where BLOB scan completes before snapshot scan starts.
     */
    TEST_F(blob_file_gc_state_machine_test, transition_blob_complete_then_snapshot) {
        assert_transition([&]() { return state_machine_.start_blob_scan(); }, state_machine_, blob_file_gc_state::scanning_blob_only);
        assert_transition([&]() { return state_machine_.complete_blob_scan(); }, state_machine_, blob_file_gc_state::blob_scan_completed_snapshot_not_started);
        assert_transition([&]() { return state_machine_.start_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal); }, state_machine_, blob_file_gc_state::blob_scan_completed_snapshot_in_progress);
        assert_transition([&]() { return state_machine_.complete_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal); }, state_machine_, blob_file_gc_state::cleaning_up);
        assert_transition([&]() { return state_machine_.complete_cleanup(); }, state_machine_, blob_file_gc_state::completed);
        assert_transition([&]() { return state_machine_.shutdown(); }, state_machine_, blob_file_gc_state::shutdown);
        assert_transition([&]() { return state_machine_.reset(); }, state_machine_, blob_file_gc_state::not_started);
    }
    
    /**
     * @brief Tests the transition where snapshot scan completes before BLOB scan starts.
     */
    TEST_F(blob_file_gc_state_machine_test, transition_snapshot_complete_then_blob) {
        assert_transition([&]() { return state_machine_.start_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal); }, state_machine_, blob_file_gc_state::scanning_snapshot_only);
        assert_transition([&]() { return state_machine_.complete_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal); }, state_machine_, blob_file_gc_state::snapshot_scan_completed_blob_not_started);
        assert_transition([&]() { return state_machine_.start_blob_scan(); }, state_machine_, blob_file_gc_state::snapshot_scan_completed_blob_in_progress);
        assert_transition([&]() { return state_machine_.complete_blob_scan(); }, state_machine_, blob_file_gc_state::cleaning_up);
        assert_transition([&]() { return state_machine_.complete_cleanup(); }, state_machine_, blob_file_gc_state::completed);
        assert_transition([&]() { return state_machine_.shutdown(); }, state_machine_, blob_file_gc_state::shutdown);
        assert_transition([&]() { return state_machine_.reset(); }, state_machine_, blob_file_gc_state::not_started);
    }
    
    /**
     * @brief Tests a cascading transition where scans complete sequentially.
     */
    TEST_F(blob_file_gc_state_machine_test, transition_cascade_completion) {
        assert_transition([&]() { return state_machine_.start_blob_scan(); }, state_machine_, blob_file_gc_state::scanning_blob_only);
        assert_transition([&]() { return state_machine_.start_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal); }, state_machine_, blob_file_gc_state::scanning_both);
        assert_transition([&]() { return state_machine_.complete_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal); }, state_machine_, blob_file_gc_state::snapshot_scan_completed_blob_in_progress);
        assert_transition([&]() { return state_machine_.complete_blob_scan(); }, state_machine_, blob_file_gc_state::cleaning_up);
        assert_transition([&]() { return state_machine_.complete_cleanup(); }, state_machine_, blob_file_gc_state::completed);
        assert_transition([&]() { return state_machine_.shutdown(); }, state_machine_, blob_file_gc_state::shutdown);
        assert_transition([&]() { return state_machine_.reset(); }, state_machine_, blob_file_gc_state::not_started);
    }
    
    /**
     * @brief Tests the transition where shutdown occurs during scanning.
     */
    TEST_F(blob_file_gc_state_machine_test, transition_shutdown_during_scanning) {
        assert_transition([&]() { return state_machine_.start_blob_scan(); }, state_machine_, blob_file_gc_state::scanning_blob_only);
        assert_transition([&]() { return state_machine_.shutdown(); }, state_machine_, blob_file_gc_state::shutdown);
        assert_transition([&]() { return state_machine_.reset(); }, state_machine_, blob_file_gc_state::not_started);
    }
    
    /**
     * @brief Tests the transition where shutdown occurs after completion.
     */
    TEST_F(blob_file_gc_state_machine_test, transition_shutdown_after_completion) {
        assert_transition([&]() { return state_machine_.start_blob_scan(); }, state_machine_, blob_file_gc_state::scanning_blob_only);
        assert_transition([&]() { return state_machine_.start_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal); }, state_machine_, blob_file_gc_state::scanning_both);
        assert_transition([&]() { return state_machine_.complete_blob_scan(); }, state_machine_, blob_file_gc_state::blob_scan_completed_snapshot_in_progress);
        assert_transition([&]() { return state_machine_.complete_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal); }, state_machine_, blob_file_gc_state::cleaning_up);
        assert_transition([&]() { return state_machine_.complete_cleanup(); }, state_machine_, blob_file_gc_state::completed);
        assert_transition([&]() { return state_machine_.shutdown(); }, state_machine_, blob_file_gc_state::shutdown);
        assert_transition([&]() { return state_machine_.reset(); }, state_machine_, blob_file_gc_state::not_started);
    }


TEST_F(blob_file_gc_state_machine_test, snapshot_scan_mode_mismatch_all) {
    // Test: internal mode started, but complete with external mode
    {
        blob_file_gc_state_machine state_machine;
        state_machine.start_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal);
        EXPECT_THROW({
            state_machine.complete_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::external);
        }, std::logic_error);
    }

    // Test: external mode started, but complete with internal mode
    {
        blob_file_gc_state_machine state_machine;
        state_machine.start_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::external);
        EXPECT_THROW({
            state_machine.complete_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal);
        }, std::logic_error);
    }

    // Test: internal mode started, but complete with none mode
    {
        blob_file_gc_state_machine state_machine;
        state_machine.start_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal);
        EXPECT_THROW({
            state_machine.complete_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::none);
        }, std::logic_error);
    }

    // Test: external mode started, but complete with none mode
    {
        blob_file_gc_state_machine state_machine;
        state_machine.start_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::external);
        EXPECT_THROW({
            state_machine.complete_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::none);
        }, std::logic_error);
    }
   
    {
        blob_file_gc_state_machine state_machine;
        EXPECT_THROW({
            state_machine.start_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::none);
        }, std::logic_error);
    }

}

TEST_F(blob_file_gc_state_machine_test, snapshot_scan_mode_match_normal_cases) {
    // Test: internal mode with matching complete call
    {
        blob_file_gc_state_machine state_machine;
        EXPECT_NO_THROW({
            state_machine.start_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal);
        });
        blob_file_gc_state new_state = blob_file_gc_state::not_started;
        EXPECT_NO_THROW({
            new_state = state_machine.complete_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::internal);
        });
        // Expected state based on the transition map:
        // snapshot_scan_completed_blob_not_started is expected when snapshot scan completes with matching mode.
        EXPECT_EQ(new_state, blob_file_gc_state::snapshot_scan_completed_blob_not_started);
    }
    
    // Test: external mode with matching complete call
    {
        blob_file_gc_state_machine state_machine;
        EXPECT_NO_THROW({
            state_machine.start_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::external);
        });
        blob_file_gc_state new_state = blob_file_gc_state::not_started;
        EXPECT_NO_THROW({
            new_state = state_machine.complete_snapshot_scan(blob_file_gc_state_machine::snapshot_scan_mode::external);
        });
        // Expected state based on the transition map:
        EXPECT_EQ(new_state, blob_file_gc_state::snapshot_scan_completed_blob_not_started);
    }
}

}  // namespace limestone::testing