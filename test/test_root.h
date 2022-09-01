/*
 * Copyright 2019-2019 tsurugi project.
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

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <boost/filesystem/path.hpp>

#include <limestone/api/datastore.h>
#include <limestone/api/configuration.h>

namespace limestone::api {

class datastore_test : public datastore {
public:
    explicit datastore_test(configuration& conf) : datastore(conf) {}
    datastore_test() : datastore() {}
    auto& log_channels() { return log_channels_; }
};

} // namespace limestone::api