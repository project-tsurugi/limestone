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

#include <cstdio>
#include <ios>
#include <vector>

#include <boost/filesystem.hpp>

#include <limestone/api/epoch_id_type.h>

namespace limestone::internal {

using limestone::api::epoch_id_type;

/**
 * @brief Preserves the epoch snippets beyond the boundary epoch of the input WAL
 *        files into a carry file.
 */
class compaction_carry_writer {
public:
    /**
     * @brief Constructs a writer with the boundary epoch and the output path.
     * @param boundary_epoch the compaction boundary epoch
     * @param out_path where the carry file is written
     */
    compaction_carry_writer(epoch_id_type boundary_epoch, boost::filesystem::path out_path);

    /**
     * @brief Reads each input from the beginning and copies the raw bytes of every
     *        snippet whose marker_begin epoch is greater than the boundary (from its
     *        marker_begin up to the next snippet header or EOF) to the output,
     *        unmodified.
     *
     * Snippets beginning with marker_invalidated_begin are not copied.
     * When no snippet crosses the boundary, the output file is not created.
     * The output is fsynced before it is closed.
     *
     * @param inputs the input WAL files (rotated and therefore complete), processed in this order
     * @return true when the output was created (= at least one snippet was copied)
     * @throws limestone_io_exception on an I/O error; a partially written output may
     *         remain (cleaning it up is the caller's responsibility)
     * @throws limestone_exception on a parse error of an input (a broken entry)
     */
    bool write(const std::vector<boost::filesystem::path>& inputs);

private:
    // Byte range of a snippet to be copied to the carry file.
    struct snippet_span {
        std::streamoff offset;
        std::streamoff length;
    };

    [[nodiscard]] std::vector<snippet_span> collect_carry_spans(const boost::filesystem::path& input) const;
    static void copy_spans(const boost::filesystem::path& input, const std::vector<snippet_span>& spans, FILE* out);

    epoch_id_type boundary_epoch_;
    boost::filesystem::path out_path_;
};

}  // namespace limestone::internal
