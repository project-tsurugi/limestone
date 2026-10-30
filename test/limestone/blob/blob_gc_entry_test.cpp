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

#include <memory>
#include <string>
#include <vector>

#include "test_root.h"

#include "blob_gc_entry.h"
#include "log_entry/log_entry.h"
#include "limestone/log/log_entry_bytes.h"

namespace limestone::testing {

using limestone::api::blob_id_type;
using limestone::api::log_entry;
using limestone::api::write_version_type;
using limestone::internal::blob_gc_entry;

// holds the key_sid, the write_version and the blob_ids of a normal_with_blob
TEST(blob_gc_entry_test, from_normal_with_blob) {
    const log_entry source = log_entry::make_normal_with_blob(2, "key", "value", write_version_type{7, 3}, {11, 12});
    const blob_gc_entry entry{source.view()};
    EXPECT_EQ(entry.serialized_key_sid(), log_entry_bytes::u64le(2) + "key");
    EXPECT_EQ(entry.write_version(), (write_version_type{7, 3}));
    EXPECT_EQ(entry.blob_ids(), (std::vector<blob_id_type>{11, 12}));
}

// returns the values after the source entry is destroyed (it owns them)
TEST(blob_gc_entry_test, owns_values) {
    auto source = std::make_unique<log_entry>(
            log_entry::make_normal_with_blob(2, "key", "value", write_version_type{7, 3}, {11}));
    const blob_gc_entry entry{source->view()};
    source.reset();
    EXPECT_EQ(entry.serialized_key_sid(), log_entry_bytes::u64le(2) + "key");
    EXPECT_EQ(entry.blob_ids(), (std::vector<blob_id_type>{11}));
}

// cannot be created from an entry other than normal_with_blob
TEST(blob_gc_entry_test, other_types_abort) {
    const write_version_type wv{7, 3};
    const log_entry normal = log_entry::make_normal_entry(1, "key", "value", wv);
    EXPECT_DEATH(blob_gc_entry{normal.view()}, "blob_gc_entry:blob_gc_entry not valid for entry type");
    const log_entry removed = log_entry::make_remove_entry(1, "key", wv);
    EXPECT_DEATH(blob_gc_entry{removed.view()}, "not valid for entry type");
    const log_entry marker = log_entry::make_marker_begin(1);
    EXPECT_DEATH(blob_gc_entry{marker.view()}, "not valid for entry type");
}

}  // namespace limestone::testing
