/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <cstdint>

namespace limestone {

/**
 * @brief logging level constant for errors
 */
static constexpr std::int32_t log_error = 10;

/**
 * @brief logging level constant for warnings
 */
static constexpr std::int32_t log_warning = 20;

/**
 * @brief logging level constant for information
 */
static constexpr std::int32_t log_info = 30;

/**
 * @brief logging level constant for debug information
 */
static constexpr std::int32_t log_debug = 40;

/**
 * @brief logging level constant for traces
 */
static constexpr std::int32_t log_trace = 50;

/**
 * @brief logging level constant to trace fine functionalities such as task scheduling
 */
static constexpr std::int32_t log_trace_fine = 70;

} // namespace
