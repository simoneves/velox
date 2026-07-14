/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License at
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

#include <cudf/io/parquet_metadata.hpp>
#include <cudf/types.hpp>

#include <string>
#include <unordered_map>

namespace facebook::velox::cudf_velox {

using ParquetColumnTypeMap = std::unordered_map<std::string, cudf::type_id>;

// Maps top-level Parquet column names to libcudf storage types. Uses the same
// schema resolution as cudf::io::read_parquet_metadata.
inline ParquetColumnTypeMap parquetColumnTypesFromMetadata(
    const cudf::io::parquet_metadata& metadata) {
  ParquetColumnTypeMap columnTypes;
  const auto& root = metadata.schema().root();
  for (int i = 0; i < root.num_children(); ++i) {
    const auto& child = root.child(i);
    if (!child.name().empty()) {
      columnTypes.emplace(child.name(), child.cudf_type().id());
    }
  }
  return columnTypes;
}

} // namespace facebook::velox::cudf_velox
