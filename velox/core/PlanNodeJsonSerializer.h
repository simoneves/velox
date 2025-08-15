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

#pragma once

#include <folly/json.h>
#include <memory>
#include <string>
#include <vector>

#include "velox/core/PlanNode.h"

namespace facebook::velox::core {

/// Enhanced JSON serialization and deserialization utilities for PlanNode
/// structures using Facebook's folly library.
class PlanNodeJsonSerializer {
 public:
  /// Options for JSON serialization
  struct SerializationOptions {
    /// Whether to pretty-print JSON with indentation
    bool prettyPrint;
    
    /// Indentation level for pretty printing
    int indentSize;
    
    /// Whether to include metadata like timestamps
    bool includeMetadata;
    
    /// Whether to sort keys alphabetically  
    bool sortKeys;
    
    /// Whether to include source code locations for debugging
    bool includeSourceLocations;
    
    /// Maximum recursion depth to prevent infinite loops
    int maxRecursionDepth;

    SerializationOptions() :
        prettyPrint(false),
        indentSize(2),
        includeMetadata(false),
        sortKeys(true),
        includeSourceLocations(false),
        maxRecursionDepth(100) {}
  };

  /// Options for JSON deserialization
  struct DeserializationOptions {
    /// Whether to validate JSON schema
    bool validateSchema;
    
    /// Whether to allow unknown fields
    bool allowUnknownFields;
    
    /// Whether to use strict type checking
    bool strictTypeChecking;
    
    /// Maximum recursion depth to prevent infinite loops
    int maxRecursionDepth;

    DeserializationOptions() :
        validateSchema(true),
        allowUnknownFields(false),
        strictTypeChecking(true),
        maxRecursionDepth(100) {}
  };

  /// Error information for serialization/deserialization failures
  struct SerializationError {
    std::string message;
    std::string context;
    int line = -1;
    int column = -1;
  };

  /// Result wrapper for serialization operations
  template<typename T>
  struct Result {
    T value;
    std::vector<SerializationError> errors;
    bool success = true;
    
    bool hasErrors() const { return !errors.empty(); }
    bool isSuccess() const { return success && !hasErrors(); }
  };

 public:
  explicit PlanNodeJsonSerializer(
      SerializationOptions serializeOpts = SerializationOptions{},
      DeserializationOptions deserializeOpts = DeserializationOptions{})
      : serializeOptions_(serializeOpts), 
        deserializeOptions_(deserializeOpts) {}

  /// Serialize a PlanNode to JSON string
  Result<std::string> serializeToJson(const PlanNodePtr& planNode) const;

  /// Serialize a PlanNode to folly::dynamic
  Result<folly::dynamic> serializeToDynamic(const PlanNodePtr& planNode) const;

  /// Deserialize a PlanNode from JSON string
  Result<PlanNodePtr> deserializeFromJson(
      const std::string& json,
      memory::MemoryPool* pool = nullptr) const;

  /// Deserialize a PlanNode from folly::dynamic
  Result<PlanNodePtr> deserializeFromDynamic(
      const folly::dynamic& dynamic,
      memory::MemoryPool* pool = nullptr) const;

  /// Validate JSON schema against expected PlanNode structure
  Result<bool> validateJsonSchema(const folly::dynamic& json) const;

  /// Convert between different JSON representations
  Result<std::string> formatJson(
      const std::string& json,
      const SerializationOptions& options) const;

  /// Extract plan statistics and metadata
  Result<folly::dynamic> extractPlanMetadata(const PlanNodePtr& planNode) const;

  /// Compare two PlanNodes for structural equality via JSON
  Result<bool> comparePlansViaJson(
      const PlanNodePtr& plan1,
      const PlanNodePtr& plan2) const;

  /// Generate a JSON schema for PlanNode validation
  static folly::dynamic generateJsonSchema();

 private:
  /// Internal helper methods
  void addMetadata(folly::dynamic& obj, const PlanNodePtr& planNode) const;
  
  bool validateNodeStructure(const folly::dynamic& node) const;
  
  void collectNodeStatistics(
      const PlanNodePtr& planNode,
      folly::dynamic& stats) const;
  
  std::string formatDynamicWithOptions(
      const folly::dynamic& dynamic,
      const SerializationOptions& options) const;
  
  Result<PlanNodePtr> deserializeWithValidation(
      const folly::dynamic& dynamic,
      memory::MemoryPool* pool,
      int currentDepth = 0) const;

  /// Add error to result
  template<typename T>
  void addError(
      Result<T>& result,
      const std::string& message,
      const std::string& context = "") const {
    result.errors.push_back({message, context});
    result.success = false;
  }

 private:
  SerializationOptions serializeOptions_;
  DeserializationOptions deserializeOptions_;
};

/// Utility functions for common JSON operations

/// Serialize a single PlanNode to pretty-printed JSON
std::string planNodeToPrettyJson(
    const PlanNodePtr& planNode,
    int indentSize = 2);

/// Deserialize a PlanNode from JSON with error handling
std::pair<PlanNodePtr, std::string> planNodeFromJson(
    const std::string& json,
    memory::MemoryPool* pool = nullptr);

/// Extract all PlanNode types used in a plan tree
std::vector<std::string> extractPlanNodeTypes(const PlanNodePtr& planNode);

/// Generate a summary of the plan structure
folly::dynamic generatePlanSummary(const PlanNodePtr& planNode);

/// Validate that a JSON string represents a valid PlanNode
bool isValidPlanNodeJson(const std::string& json);

} // namespace facebook::velox::core