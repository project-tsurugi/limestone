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
 
#include "blob_item_container.h"
#include <stdexcept>
#include <algorithm>

namespace limestone::api {

//==============================================================================
// blob_item Implementation
//==============================================================================

blob_item::blob_item(blob_id_type blob_id)
    : blob_id_(blob_id)
{
}

blob_id_type blob_item::get_blob_id() const {
    return blob_id_;
}

//==============================================================================
// blob_item_container Implementation
//==============================================================================

void blob_item_container::add_blob_item(const blob_item &item) {
    if (iterator_used_) {
        throw std::logic_error("Cannot modify blob_item_container once an iterator has been obtained.");
    }
    items_.push_back(item);
}

void blob_item_container::diff(const blob_item_container &other) {
    if (iterator_used_) {
        throw std::logic_error("Cannot modify blob_item_container once an iterator has been obtained.");
    }
    // If 'other' is the same container as 'this', then clear the container.
    if (&other == this) {
        items_.clear();
        return;
    }
    
    // Move current items to a local variable and clear items_
    container_type old_items = std::move(items_);
    items_.clear();

    // Sort old_items by blob_id.
    std::sort(old_items.begin(), old_items.end(), [](const blob_item &a, const blob_item &b) {
        return a.get_blob_id() < b.get_blob_id();
    });

    // Assume that 'other.items_' is already sorted (because its begin() has been called or by contract).
    // We now perform a merge-like set difference: new items_ will contain all elements from old_items that
    // are NOT present in other.items_. Duplicates are eliminated in the process.
    auto it1 = old_items.begin();
    auto end1 = old_items.end();
    auto it2 = other.items_.begin();
    auto end2 = other.items_.end();

    while (it1 != end1) {
        // Advance it2 until it2 is at end or *it2 is not less than *it1.
        while (it2 != end2 && it2->get_blob_id() < it1->get_blob_id()) {
            ++it2;
        }
        // If it2 is not at end and *it1 equals *it2, then skip this element.
        if (it2 != end2 && it2->get_blob_id() == it1->get_blob_id()) {
            ++it1;
            continue;
        }
        // Otherwise, add *it1 to items_ if not a duplicate.
        if (items_.empty() || items_.back().get_blob_id() != it1->get_blob_id()) {
            items_.push_back(*it1);
        }
        ++it1;
    }
}


void blob_item_container::merge(const blob_item_container &other) {
    if (iterator_used_) {
        throw std::logic_error("Cannot modify blob_item_container once an iterator has been obtained.");
    }
    // Add all items from 'other' into this container.
    for (const auto &item : other.items_) {
        items_.push_back(item);
    }
    sort();
}

typename blob_item_container::const_iterator blob_item_container::begin() const {
    // In order to guarantee that the container is read-only once an iterator is obtained,
    // we need to update the internal flag 'iterator_used_'.
    // However, since this function is declared as const, direct modification of member
    // variables is not allowed. Therefore, we use const_cast to remove the constness temporarily.
    //
    // The 'self' pointer is used to:
    //   1. Clarify that we are working with a non-const version of the object for internal state updates.
    //   2. Update the 'iterator_used_' flag to true, which will prevent any further modifications
    //      (e.g., add_blob_item, diff, merge) to the container after an iterator is obtained.
    //
    // This operation does not change the observable state of the container from the client's point
    // of view, because the container is intended to be read-only once an iterator is retrieved.
    blob_item_container* self = const_cast<blob_item_container*>(this);
    if (!self->iterator_used_) {
        self->sort();
        self->iterator_used_ = true;
    }
    return items_.cbegin();
}

typename blob_item_container::const_iterator blob_item_container::end() const {
    return items_.cend();
}

void blob_item_container::sort() {
    // Sort items using the operator< defined for blob_item.
    std::sort(items_.begin(), items_.end(), [](const blob_item &a, const blob_item &b) {
        return a.get_blob_id() < b.get_blob_id();
    });
}


} // namespace limestone::api
