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

#include <ostream>
#include <string>
#include <string_view>

#include "compaction_test_fixture.h"

namespace limestone::testing {

/**
 * @brief the way a compaction is performed on the log directory.
 *
 * The compaction scenarios below are run once per mode, so that a scenario added for one
 * mode is automatically exercised by the other as well. Both modes share the same core
 * logic that merges the pwal files into the compacted file, and must therefore produce
 * the same compacted file, the same catalog contents and the same snapshot; they differ
 * only in how the log directory around it is rebuilt (see the accessors below).
 */
enum class compaction_mode {
    /// @brief compaction performed by a running datastore (datastore::compact_with_online).
    online,
    /// @brief compaction performed by the tglogutil command while the datastore is stopped.
    offline,
};

std::string_view to_string_view(compaction_mode mode);

std::ostream& operator<<(std::ostream& os, compaction_mode mode);

/**
 * @brief fixture running a compaction scenario against both online and offline compaction.
 *
 * A scenario writes log entries through the datastore, calls run_compaction() and checks the
 * outcome. run_compaction() dispatches to the mode under test, so the mode-independent
 * expectations (compacted file contents, catalog, snapshot, blob files still referenced)
 * are stated once and verified for both modes. The known and intended differences between
 * the two modes are exposed through retains_detached_pwals() and records_rotation_epoch(), so
 * that a scenario states them explicitly instead of hiding them.
 */
class compaction_scenario_test : public compaction_test,
                                 public ::testing::WithParamInterface<compaction_mode> {
public:
    // Offline compaction creates its work and backup directories next to the log directory,
    // so keep the log directory one level down: the siblings then stay inside test_root_
    // instead of polluting /tmp, which unrelated processes churn.
    static constexpr char const* test_root_ = "/tmp/compaction_scenario_test";

    compaction_scenario_test() : compaction_test("/tmp/compaction_scenario_test/log_dir") {}

    // Recreate the per-suite root before each test and remove it afterwards, so that a work
    // or backup directory left behind by an aborted run can never be observed by a later test.
    void SetUp() override {
        boost::filesystem::remove_all(test_root_);
        boost::filesystem::create_directories(test_root_);
        compaction_test::SetUp();
    }

    void TearDown() override {
        compaction_test::TearDown();
        boost::filesystem::remove_all(test_root_);
    }

    /**
     * @brief performs a compaction on the log directory in the mode under test.
     *
     * In online mode the datastore stays up and the compaction runs concurrently with a
     * switch to @p epoch. In offline mode the datastore is shut down, the tglogutil command
     * is invoked, the datastore is restarted and then switched to @p epoch, so that both
     * modes leave the datastore running at the same epoch.
     *
     * The datastore is recreated in offline mode, so lc0_, lc1_ and lc2_ are rebound by this
     * call: a scenario must not cache log_channel pointers across it.
     * @param epoch the epoch the datastore is switched to as part of the compaction
     */
    void run_compaction(epoch_id_type epoch);

    /**
     * @brief tells whether the compaction under test keeps the source pwal files.
     *
     * Online compaction rotates the source pwal files, keeps them in the log directory and
     * records them in the catalog as detached pwals. Offline compaction rebuilds the log
     * directory from scratch, so the source pwal files are gone and the catalog records no
     * detached pwals. This difference is intended.
     * @return true if the source pwal files survive as detached pwals
     */
    bool retains_detached_pwals() const { return GetParam() == compaction_mode::online; }

    /**
     * @brief tells whether the compaction records the epoch it rotated at in the catalog.
     *
     * Online compaction rotates the pwal files of a running datastore and records the epoch of
     * that rotation as max_epoch_id. Offline compaction has only the epoch file to go by, and
     * records the durable epoch it reads from it. An epoch that carries no data never reaches the
     * epoch file, so the durable epoch can lag the epoch the datastore had reached, and the two
     * modes then record a different max_epoch_id for the same data. Both values denote an epoch
     * the compacted data is durable at; max_epoch_id only guards whether blob garbage collection
     * may run. This difference is inherent to compacting a stopped log directory.
     * @return true if the epoch of the rotation is recorded instead of the durable epoch
     */
    bool records_rotation_epoch() const { return GetParam() == compaction_mode::online; }
};

}  // namespace limestone::testing
