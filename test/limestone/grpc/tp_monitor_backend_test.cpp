/*
 * Copyright 2026 Project Tsurugi.
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

#include <string>

#include <grpc/backend/tp_monitor_backend.h>
#include <test_root.h>

namespace limestone::testing {

class tp_monitor_backend_test : public ::testing::Test {};

TEST_F(tp_monitor_backend_test, create_with_zero_participant_count) { // NOLINT
    limestone::grpc::backend::tp_monitor_backend backend{};
    auto result = backend.create(0);
    EXPECT_TRUE(! result.ok);
    EXPECT_EQ(result.tpm_id, 0U);
}

TEST_F(tp_monitor_backend_test, join_duplicate_ts_id_is_ignored) { // NOLINT
    limestone::grpc::backend::tp_monitor_backend backend{};
    auto create_result = backend.create(1);
    ASSERT_TRUE(create_result.ok);

    auto join_result = backend.join(create_result.tpm_id, "ts-1");
    EXPECT_TRUE(join_result.ok);

    auto duplicate_result = backend.join(create_result.tpm_id, "ts-1");
    EXPECT_TRUE(! duplicate_result.ok);
}

TEST_F(tp_monitor_backend_test, barrier_notify_requires_join) { // NOLINT
    limestone::grpc::backend::tp_monitor_backend backend{};
    auto create_result = backend.create(1);
    ASSERT_TRUE(create_result.ok);

    auto pre_notify = backend.barrier_notify(create_result.tpm_id, "ts-1");
    EXPECT_TRUE(! pre_notify.ok);

    auto join_result = backend.join(create_result.tpm_id, "ts-1");
    ASSERT_TRUE(join_result.ok);

    auto notify_result = backend.barrier_notify(create_result.tpm_id, "ts-1");
    EXPECT_TRUE(notify_result.ok);
}

} // namespace limestone::testing
