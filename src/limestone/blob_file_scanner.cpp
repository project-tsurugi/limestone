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

#include "blob_file_scanner.h"

#include "blob_file_resolver.h"
#include "limestone/logging.h"
#include "logging_helper.h"

namespace limestone::internal {

blob_file_scanner::blob_file_scanner(const blob_file_resolver* resolver)
    : resolver_(resolver) {}

blob_file_scanner::iterator::iterator() : resolver_(nullptr) {}

blob_file_scanner::iterator::iterator(boost::filesystem::recursive_directory_iterator iter, const blob_file_resolver* resolver)
    : iter_(std::move(iter)), resolver_(resolver)
{
    skip_non_blob_files();
}

blob_file_scanner::iterator& blob_file_scanner::iterator::operator++() {
    ++iter_;
    skip_non_blob_files();
    return *this;
 }
 
 blob_file_scanner::iterator::reference
 blob_file_scanner::iterator::operator*() const {
     return iter_->path();
 }
 
 bool blob_file_scanner::iterator::operator!=(const iterator& other) const {
     return iter_ != other.iter_;
 }
 
 void blob_file_scanner::iterator::skip_non_blob_files() {
     while (iter_ != boost::filesystem::recursive_directory_iterator() &&
            (!boost::filesystem::is_regular_file(iter_->path()) ||
             !resolver_->is_blob_file(iter_->path()))) {
         ++iter_;
     }
 }
 
blob_file_scanner::iterator blob_file_scanner::begin() const {
    return iterator{boost::filesystem::recursive_directory_iterator(resolver_->get_blob_root()), resolver_};
}
 
 blob_file_scanner::iterator blob_file_scanner::end() const {
     return {};
 }

 }  // namespace limestone::internal
