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
#include "blob_pool_impl.h"

namespace limestone::internal {

blob_pool_impl::blob_pool_impl(std::function<blob_id_type()> id_generator,
                               limestone::internal::blob_file_resolver& resolver)
    : id_generator_(std::move(id_generator)), resolver_(resolver) {}

blob_id_type blob_pool_impl::generate_blob_id() {
    return id_generator_();
}

void blob_pool_impl::release() {
    // 空実装
}

blob_id_type blob_pool_impl::register_file(boost::filesystem::path const& /*file*/, bool /*is_temporary_file*/) {
    return generate_blob_id(); // ダミーとして新しいIDを返す
}

blob_id_type blob_pool_impl::register_data(std::string_view /*data*/) {
    return generate_blob_id(); // ダミーとして新しいIDを返す
}

blob_id_type blob_pool_impl::duplicate_data(blob_id_type /*reference*/) {
    return generate_blob_id(); // ダミーとして新しいIDを返す
}

} // namespace limestone::internal
