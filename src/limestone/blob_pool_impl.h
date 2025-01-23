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

#pragma once

#include <limestone/api/blob_pool.h>
#include <functional>
#include "blob_file_resolver.h"

namespace limestone::internal {

using namespace  limestone::api;


/**
 * @brief Implementation of the blob_pool interface.
 */
class blob_pool_impl : public blob_pool {
public:
    /**
     * @brief Constructs a blob_pool_impl instance with the given ID generator and blob_file_resolver.
     * @param id_generator A callable object that generates unique IDs of type blob_id_type.
     * @param resolver Reference to a blob_file_resolver instance.
     */
    explicit blob_pool_impl(std::function<blob_id_type()> id_generator,
                            limestone::internal::blob_file_resolver& resolver);

    void release() override;

    [[nodiscard]] blob_id_type register_file(boost::filesystem::path const& file,
                                             bool is_temporary_file) override;

    [[nodiscard]] blob_id_type register_data(std::string_view data) override;

    [[nodiscard]] blob_id_type duplicate_data(blob_id_type reference) override;


private:
    /**
     * @brief Generates a unique ID for a BLOB.
     * 
     * @return A unique ID of type blob_id_type.
     */
    [[nodiscard]] blob_id_type generate_blob_id();

    std::function<blob_id_type()> id_generator_; // Callable object for ID generation

    blob_file_resolver& resolver_; // reference to a blob_file_resolver instance
};

} // namespace limestone::internal