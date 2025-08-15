/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include "velox/core/PlanNodeJsonSerializer.h"

#include <folly/json.h>
#include <chrono>
#include <iostream>
#include <sstream>

#include "velox/common/base/Exceptions.h"
#include "velox/common/serialization/Serializable.h"

namespace facebook::velox::core {

namespace {

folly::json::serialization_opts createSerializationOpts(
    const PlanNodeJsonSerializer::SerializationOptions& options) {
  folly::json::serialization_opts opts;
  opts.pretty_formatting = options.prettyPrint;
  opts.sort_keys = options.sortKeys;
  opts.allow_nan_inf = true;
  opts.allow_trailing_comma = true;
  opts.recursion_limit = options.maxRecursionDepth;
  
  if (options.prettyPrint) {
    opts.pretty_formatting_indent_width = options.indentSize;
  }
  
  return opts;
}

folly::json::parse_opts createParseOpts(
    const PlanNodeJsonSerializer::DeserializationOptions& options) {
  folly::json::parse_opts opts;
  opts.allow_trailing_comma = true;
  opts.recursion_limit = options.maxRecursionDepth;
  return opts;
}

} // namespace

PlanNodeJsonSerializer::Result<std::string> 
PlanNodeJsonSerializer::serializeToJson(const PlanNodePtr& planNode) const {
  Result<std::string> result;
  
  if (!planNode) {
    addError(result, "PlanNode is null", "serializeToJson");
    return result;
  }

  try {
    auto dynamicResult = serializeToDynamic(planNode);
    if (!dynamicResult.isSuccess()) {
      result.errors = std::move(dynamicResult.errors);
      result.success = false;
      return result;
    }

    auto opts = createSerializationOpts(serializeOptions_);
    result.value = folly::json::serialize(dynamicResult.value, opts);
    
  } catch (const std::exception& e) {
    addError(result, 
        fmt::format("JSON serialization failed: {}", e.what()),
        "serializeToJson");
  }

  return result;
}

PlanNodeJsonSerializer::Result<folly::dynamic> 
PlanNodeJsonSerializer::serializeToDynamic(const PlanNodePtr& planNode) const {
  Result<folly::dynamic> result;
  
  if (!planNode) {
    addError(result, "PlanNode is null", "serializeToDynamic");
    return result;
  }

  try {
    // Use the existing Velox serialization infrastructure
    result.value = planNode->serialize();
    
    // Add metadata if requested
    if (serializeOptions_.includeMetadata) {
      addMetadata(result.value, planNode);
    }
    
    // Add source location information if requested
    if (serializeOptions_.includeSourceLocations) {
      result.value["_metadata"]["serialized_at"] = 
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch()).count();
      result.value["_metadata"]["serializer_version"] = "1.0";
    }
    
  } catch (const std::exception& e) {
    addError(result,
        fmt::format("Dynamic serialization failed: {}", e.what()),
        "serializeToDynamic");
  }

  return result;
}

PlanNodeJsonSerializer::Result<PlanNodePtr> 
PlanNodeJsonSerializer::deserializeFromJson(
    const std::string& json,
    memory::MemoryPool* pool) const {
  Result<PlanNodePtr> result;
  
  if (json.empty()) {
    addError(result, "JSON string is empty", "deserializeFromJson");
    return result;
  }

  try {
    auto opts = createParseOpts(deserializeOptions_);
    auto dynamic = folly::json::parse(json, opts);
    
    return deserializeFromDynamic(dynamic, pool);
    
  } catch (const std::exception& e) {
    addError(result,
        fmt::format("JSON parsing failed: {}", e.what()),
        "deserializeFromJson");
  }

  return result;
}

PlanNodeJsonSerializer::Result<PlanNodePtr> 
PlanNodeJsonSerializer::deserializeFromDynamic(
    const folly::dynamic& dynamic,
    memory::MemoryPool* pool) const {
  
  if (deserializeOptions_.validateSchema) {
    auto validationResult = validateJsonSchema(dynamic);
    if (!validationResult.isSuccess()) {
      Result<PlanNodePtr> result;
      result.errors = std::move(validationResult.errors);
      result.success = false;
      return result;
    }
  }

  return deserializeWithValidation(dynamic, pool);
}

PlanNodeJsonSerializer::Result<PlanNodePtr> 
PlanNodeJsonSerializer::deserializeWithValidation(
    const folly::dynamic& dynamic,
    memory::MemoryPool* pool,
    int currentDepth) const {
  Result<PlanNodePtr> result;
  
  if (currentDepth > deserializeOptions_.maxRecursionDepth) {
    addError(result, "Maximum recursion depth exceeded", "deserializeWithValidation");
    return result;
  }

  try {
    // Use the existing Velox deserialization infrastructure
    result.value = ISerializable::deserialize<PlanNode>(dynamic, pool);
    
  } catch (const std::exception& e) {
    addError(result,
        fmt::format("Deserialization failed: {}", e.what()),
        "deserializeWithValidation");
  }

  return result;
}

PlanNodeJsonSerializer::Result<bool> 
PlanNodeJsonSerializer::validateJsonSchema(const folly::dynamic& json) const {
  Result<bool> result;
  result.value = true;

  if (!json.isObject()) {
    addError(result, "Root element must be an object", "validateJsonSchema");
    return result;
  }

  // Check required fields
  if (!json.count("name")) {
    addError(result, "Missing required field 'name'", "validateJsonSchema");
  }
  
  if (!json.count("id")) {
    addError(result, "Missing required field 'id'", "validateJsonSchema");
  }

  // Validate node structure recursively
  if (!validateNodeStructure(json)) {
    addError(result, "Invalid node structure", "validateJsonSchema");
  }

  result.value = result.isSuccess();
  return result;
}

bool PlanNodeJsonSerializer::validateNodeStructure(const folly::dynamic& node) const {
  if (!node.isObject()) {
    return false;
  }

  // Check if sources exist and are valid
  if (node.count("sources") && node["sources"].isArray()) {
    for (const auto& source : node["sources"]) {
      if (!validateNodeStructure(source)) {
        return false;
      }
    }
  }

  return true;
}

PlanNodeJsonSerializer::Result<std::string> 
PlanNodeJsonSerializer::formatJson(
    const std::string& json,
    const SerializationOptions& options) const {
  Result<std::string> result;
  
  try {
    auto opts = createParseOpts(deserializeOptions_);
    auto dynamic = folly::json::parse(json, opts);
    
    result.value = formatDynamicWithOptions(dynamic, options);
    
  } catch (const std::exception& e) {
    addError(result,
        fmt::format("JSON formatting failed: {}", e.what()),
        "formatJson");
  }

  return result;
}

std::string PlanNodeJsonSerializer::formatDynamicWithOptions(
    const folly::dynamic& dynamic,
    const SerializationOptions& options) const {
  auto opts = createSerializationOpts(options);
  return folly::json::serialize(dynamic, opts);
}

PlanNodeJsonSerializer::Result<folly::dynamic> 
PlanNodeJsonSerializer::extractPlanMetadata(const PlanNodePtr& planNode) const {
  Result<folly::dynamic> result;
  
  if (!planNode) {
    addError(result, "PlanNode is null", "extractPlanMetadata");
    return result;
  }

  try {
    folly::dynamic metadata = folly::dynamic::object;
    collectNodeStatistics(planNode, metadata);
    result.value = std::move(metadata);
    
  } catch (const std::exception& e) {
    addError(result,
        fmt::format("Metadata extraction failed: {}", e.what()),
        "extractPlanMetadata");
  }

  return result;
}

void PlanNodeJsonSerializer::collectNodeStatistics(
    const PlanNodePtr& planNode,
    folly::dynamic& stats) const {
  if (!planNode) return;

  // Initialize statistics
  if (!stats.count("nodeCount")) {
    stats["nodeCount"] = 0;
    stats["nodeTypes"] = folly::dynamic::object;
    stats["maxDepth"] = 0;
    stats["totalNodes"] = 0;
  }

  // Count this node
  stats["totalNodes"] = stats["totalNodes"].asInt() + 1;
  
  // Count node types
  std::string nodeType = planNode->name();
  if (!stats["nodeTypes"].count(nodeType)) {
    stats["nodeTypes"][nodeType] = 0;
  }
  stats["nodeTypes"][nodeType] = stats["nodeTypes"][nodeType].asInt() + 1;

  // Recursively process children
  for (const auto& source : planNode->sources()) {
    collectNodeStatistics(source, stats);
  }
}

void PlanNodeJsonSerializer::addMetadata(
    folly::dynamic& obj,
    const PlanNodePtr& planNode) const {
  obj["_metadata"] = folly::dynamic::object;
  obj["_metadata"]["nodeType"] = planNode->name();
  obj["_metadata"]["outputFields"] = static_cast<int64_t>(planNode->outputType()->size());
  obj["_metadata"]["sourceCount"] = static_cast<int64_t>(planNode->sources().size());
}

PlanNodeJsonSerializer::Result<bool> 
PlanNodeJsonSerializer::comparePlansViaJson(
    const PlanNodePtr& plan1,
    const PlanNodePtr& plan2) const {
  Result<bool> result;
  
  try {
    auto json1Result = serializeToDynamic(plan1);
    auto json2Result = serializeToDynamic(plan2);
    
    if (!json1Result.isSuccess() || !json2Result.isSuccess()) {
      result.errors.insert(result.errors.end(), 
                          json1Result.errors.begin(), json1Result.errors.end());
      result.errors.insert(result.errors.end(),
                          json2Result.errors.begin(), json2Result.errors.end());
      result.success = false;
      return result;
    }

    // Remove metadata for comparison
    auto obj1 = json1Result.value;
    auto obj2 = json2Result.value;
    
    if (obj1.count("_metadata")) {
      obj1.erase("_metadata");
    }
    if (obj2.count("_metadata")) {
      obj2.erase("_metadata");
    }

    result.value = (obj1 == obj2);
    
  } catch (const std::exception& e) {
    addError(result,
        fmt::format("Plan comparison failed: {}", e.what()),
        "comparePlansViaJson");
  }

  return result;
}

folly::dynamic PlanNodeJsonSerializer::generateJsonSchema() {
  folly::dynamic schema = folly::dynamic::object;
  
  schema["$schema"] = "http://json-schema.org/draft-07/schema#";
  schema["title"] = "PlanNode JSON Schema";
  schema["type"] = "object";
  
  schema["required"] = folly::dynamic::array("name", "id");
  
  schema["properties"] = folly::dynamic::object;
  schema["properties"]["name"] = folly::dynamic::object;
  schema["properties"]["name"]["type"] = "string";
  schema["properties"]["name"]["description"] = "The type name of the plan node";
  
  schema["properties"]["id"] = folly::dynamic::object;
  schema["properties"]["id"]["type"] = "string";
  schema["properties"]["id"]["description"] = "Unique identifier for the plan node";
  
  schema["properties"]["sources"] = folly::dynamic::object;
  schema["properties"]["sources"]["type"] = "array";
  schema["properties"]["sources"]["description"] = "Child plan nodes";
  schema["properties"]["sources"]["items"] = folly::dynamic::object;
  schema["properties"]["sources"]["items"]["$ref"] = "#";
  
  return schema;
}

// Utility functions implementation

std::string planNodeToPrettyJson(const PlanNodePtr& planNode, int indentSize) {
  PlanNodeJsonSerializer::SerializationOptions opts;
  opts.prettyPrint = true;
  opts.indentSize = indentSize;
  opts.sortKeys = true;
  
  PlanNodeJsonSerializer serializer(opts);
  auto result = serializer.serializeToJson(planNode);
  
  if (!result.isSuccess()) {
    return fmt::format("{{\"error\": \"Serialization failed: {}\"}}",
                      result.errors.empty() ? "Unknown error" : result.errors[0].message);
  }
  
  return result.value;
}

std::pair<PlanNodePtr, std::string> planNodeFromJson(
    const std::string& json,
    memory::MemoryPool* pool) {
  PlanNodeJsonSerializer::DeserializationOptions opts;
  opts.validateSchema = true;
  opts.allowUnknownFields = false;
  
  PlanNodeJsonSerializer serializer({}, opts);
  auto result = serializer.deserializeFromJson(json, pool);
  
  std::string error;
  if (!result.isSuccess()) {
    error = result.errors.empty() ? "Unknown error" : result.errors[0].message;
  }
  
  return {result.value, error};
}

std::vector<std::string> extractPlanNodeTypes(const PlanNodePtr& planNode) {
  std::vector<std::string> types;
  std::function<void(const PlanNodePtr&)> visit = [&](const PlanNodePtr& node) {
    if (!node) return;
    types.push_back(node->name());
    for (const auto& source : node->sources()) {
      visit(source);
    }
  };
  
  visit(planNode);
  return types;
}

folly::dynamic generatePlanSummary(const PlanNodePtr& planNode) {
  folly::dynamic summary = folly::dynamic::object;
  
  if (!planNode) {
    summary["error"] = "PlanNode is null";
    return summary;
  }

  summary["rootNodeType"] = planNode->name();
  summary["rootNodeId"] = planNode->id();
  summary["outputFields"] = static_cast<int64_t>(planNode->outputType()->size());
  
  // Count nodes and types
  auto nodeTypes = extractPlanNodeTypes(planNode);
  summary["totalNodes"] = static_cast<int64_t>(nodeTypes.size());
  
  std::map<std::string, int> typeCounts;
  for (const auto& type : nodeTypes) {
    typeCounts[type]++;
  }
  
  folly::dynamic typeCountObj = folly::dynamic::object;
  for (const auto& [type, count] : typeCounts) {
    typeCountObj[type] = count;
  }
  summary["nodeTypeCounts"] = std::move(typeCountObj);
  
  return summary;
}

bool isValidPlanNodeJson(const std::string& json) {
  try {
    PlanNodeJsonSerializer serializer;
    auto parseResult = folly::json::parse(json);
    auto validationResult = serializer.validateJsonSchema(parseResult);
    return validationResult.isSuccess();
  } catch (...) {
    return false;
  }
}

} // namespace facebook::velox::core