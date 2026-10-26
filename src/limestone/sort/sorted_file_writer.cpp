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
#include "sort/sorted_file_writer.h"

#include <cerrno>
#include <unistd.h>
#include <utility>

#include <glog/logging.h>

#include <limestone/api/limestone_exception.h>
#include <limestone/logging.h>

#include "limestone_exception_helper.h"
#include "log_entry.h"
#include "logging_helper.h"

namespace limestone::internal {

using limestone::api::log_entry;

namespace {

/**
 * @brief Creates and opens a sorted file.
 * @param file the file to create
 * @param buffer the stdio buffer. It must be valid until the stream is closed
 * @return the opened stream
 * @throws limestone_io_exception if the file cannot be opened (with errno)
 */
FILE* open_sorted_file(const boost::filesystem::path& file, std::string& buffer) {
    FILE* stream = fopen(file.c_str(), "wb");  // NOLINT(*-owning-memory)
    if (stream == nullptr) {
        LOG_AND_THROW_IO_EXCEPTION("cannot create sorted file: " + file.string(), errno);
    }
    // the return value is not checked: the stream works with the default buffer if it fails
    (void) setvbuf(stream, buffer.data(), _IOFBF, buffer.size());
    return stream;
}

}  // namespace

sorted_file_writer::sorted_file_writer(boost::filesystem::path file)
    : file_(std::move(file)),
      stream_buffer_(stream_buffer_size, '\0'),
      stream_(open_sorted_file(file_, stream_buffer_)) {}

sorted_file_writer::~sorted_file_writer() {
    if (stream_ != nullptr) {
        (void) fclose(stream_);  // NOLINT(*-owning-memory) cleanup without close(); failure ignored
    }
}

void sorted_file_writer::write(const log_entry_view& entry) {
    if (stream_ == nullptr) {
        LOG_LP(FATAL) << "already closed: " << file_;
    }
    if (! deduplicator_.accept(entry)) {
        return;
    }
    log_entry::write_serialized(stream_, entry, log_entry::write_version_handling::keep);
    ++written_count_;
}

void sorted_file_writer::close() {
    if (stream_ == nullptr) {
        LOG_LP(FATAL) << "already closed: " << file_;
    }
    FILE* stream = stream_;
    stream_ = nullptr;
    if (fflush(stream) != 0) {
        const int saved_errno = errno;
        (void) fclose(stream);  // NOLINT(*-owning-memory) cleanup before the throw; failure ignored
        LOG_AND_THROW_IO_EXCEPTION("cannot flush sorted file: " + file_.string(), saved_errno);
    }
    if (fsync(fileno(stream)) != 0) {
        const int saved_errno = errno;
        (void) fclose(stream);  // NOLINT(*-owning-memory) cleanup before the throw; failure ignored
        LOG_AND_THROW_IO_EXCEPTION("cannot fsync sorted file: " + file_.string(), saved_errno);
    }
    if (fclose(stream) != 0) {  // NOLINT(*-owning-memory)
        LOG_AND_THROW_IO_EXCEPTION("cannot close sorted file: " + file_.string(), errno);
    }
}

std::uint64_t sorted_file_writer::written_count() const noexcept {
    return written_count_;
}

}  // namespace limestone::internal
