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
 
#include "blob_id_container.h"
#include <stdexcept>
#include <algorithm>
#include <sstream> // added for debug_string

namespace limestone::internal {

void blob_id_container::add_blob_id(blob_id_type id) {
    if (iterator_used_) {
        throw std::logic_error("Cannot modify blob_id_container once an iterator has been obtained.");
    }
    ids_.push_back(id);
}

void blob_id_container::diff(const blob_id_container &other) {
    if (iterator_used_) {
        throw std::logic_error("Cannot modify blob_id_container once an iterator has been obtained.");
    }
    // If 'other' is the same container as 'this', then clear the container.
    if (&other == this) {
        ids_.clear();
        return;
    }
    
    container_type old_ids = std::move(ids_);
    ids_.clear();
    std::sort(old_ids.begin(), old_ids.end());
    
    container_type sorted_other = other.ids_;
    std::sort(sorted_other.begin(), sorted_other.end());

    auto it1 = old_ids.begin();
    auto end1 = old_ids.end();
    auto it2 = sorted_other.begin();
    auto end2 = sorted_other.end();

    while (it1 != end1) {
        while (it2 != end2 && *it2 < *it1) {
            ++it2;
        }
        if (it2 != end2 && *it1 == *it2) {
            ++it1;
            continue;
        }
        if (ids_.empty() || ids_.back() != *it1) {
            ids_.push_back(*it1);
        }
        ++it1;
    }
}

void blob_id_container::merge(const blob_id_container &other) {
    if (iterator_used_) {
        throw std::logic_error("Cannot modify blob_id_container once an iterator has been obtained.");
    }
    for (const auto &id : other.ids_) {
        ids_.push_back(id);
    }
    sort();
}

typename blob_id_container::const_iterator blob_id_container::begin() const {
    auto self = const_cast<blob_id_container*>(this);
    if (!self->iterator_used_) {
        self->sort();
        self->iterator_used_ = true;
    }
    return ids_.cbegin();
}

typename blob_id_container::const_iterator blob_id_container::end() const {
    return ids_.cend();
}

void blob_id_container::sort() {
    std::sort(ids_.begin(), ids_.end());
}

std::string blob_id_container::debug_string() const {
    std::ostringstream oss;
    oss << "[";
    for (std::size_t i = 0; i < ids_.size(); ++i) {
        oss << ids_[i];
        if (i + 1 < ids_.size()) {
            oss << ", ";
        }
    }
    oss << "]";
    return oss.str();
}

} // namespace limestone::internal
