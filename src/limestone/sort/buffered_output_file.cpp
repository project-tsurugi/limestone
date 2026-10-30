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
#include "sort/buffered_output_file.h"

#include <unistd.h>

#include <cerrno>
#include <utility>

#include <glog/logging.h>

#include <limestone/api/limestone_exception.h>
#include <limestone/logging.h>

#include "limestone_exception_helper.h"
#include "logging_helper.h"

namespace limestone::internal {

namespace {

/**
 * @brief Creates and opens a file, and sets its stdio buffer.
 * @param file the file to create
 * @param label the name of the kind of the file used in error messages
 * @param buffer the stdio buffer. It must be valid until the stream is closed
 * @return the opened stream
 * @throws limestone_io_exception if the file cannot be opened (with errno)
 */
FILE* open_file(const boost::filesystem::path& file, const std::string& label, std::string& buffer) {
    FILE* stream = fopen(file.c_str(), "wb");  // NOLINT(*-owning-memory)
    if (stream == nullptr) {
        LOG_AND_THROW_IO_EXCEPTION("cannot create " + label + ": " + file.string(), errno);
    }
    // the return value is not checked: the stream works with the default buffer if it fails
    (void) setvbuf(stream, buffer.data(), _IOFBF, buffer.size());
    return stream;
}

}  // namespace

buffered_output_file::buffered_output_file(boost::filesystem::path file, std::string label)
    : file_(std::move(file)),
      label_(std::move(label)),
      stream_buffer_(stream_buffer_size, '\0'),
      stream_(open_file(file_, label_, stream_buffer_)) {}

buffered_output_file::~buffered_output_file() {
    if (stream_ != nullptr) {
        (void) fclose(stream_);  // NOLINT(*-owning-memory) cleanup without close(); failure ignored
    }
}

FILE* buffered_output_file::stream() const {
    if (stream_ == nullptr) {
        LOG_LP(FATAL) << "already closed: " << file_;
    }
    return stream_;
}

bool buffered_output_file::is_open() const noexcept {
    return stream_ != nullptr;
}

void buffered_output_file::close() {
    if (stream_ == nullptr) {
        LOG_LP(WARNING) << "already closed: " << file_;
        return;
    }
    finish(false);
}

void buffered_output_file::sync_and_close() {
    if (stream_ == nullptr) {
        if (! synced_) {
            LOG_LP(FATAL) << "already closed without fsync: " << file_;
        }
        LOG_LP(WARNING) << "already closed: " << file_;
        return;
    }
    finish(true);
}

const boost::filesystem::path& buffered_output_file::file() const noexcept {
    return file_;
}

void buffered_output_file::finish(bool sync) {
    FILE* stream = stream_;
    stream_ = nullptr;
    if (fflush(stream) != 0) {
        const int saved_errno = errno;
        (void) fclose(stream);  // NOLINT(*-owning-memory) cleanup before the throw; failure ignored
        LOG_AND_THROW_IO_EXCEPTION("cannot flush " + label_ + ": " + file_.string(), saved_errno);
    }
    if (sync && fsync(fileno(stream)) != 0) {
        const int saved_errno = errno;
        (void) fclose(stream);  // NOLINT(*-owning-memory) cleanup before the throw; failure ignored
        LOG_AND_THROW_IO_EXCEPTION("cannot fsync " + label_ + ": " + file_.string(), saved_errno);
    }
    if (fclose(stream) != 0) {  // NOLINT(*-owning-memory)
        LOG_AND_THROW_IO_EXCEPTION("cannot close " + label_ + ": " + file_.string(), errno);
    }
    synced_ = sync;
}

}  // namespace limestone::internal
