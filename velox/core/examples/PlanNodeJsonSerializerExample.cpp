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

/**
 * Example usage of PlanNodeJsonSerializer for serializing and deserializing
 * Velox PlanNode structures to/from JSON using Facebook's folly library.
 */

#include "velox/core/PlanNodeJsonSerializer.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <iostream>

using namespace facebook::velox;

class PlanNodeJsonExample : public test::VectorTestBase {
 public:
  PlanNodeJsonExample() {
    // Initialize required registrations
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();
    
    // Register serialization/deserialization handlers
    Type::registerSerDe();
    core::PlanNode::registerSerDe();
    core::ITypedExpr::registerSerDe();
    
    createSampleData();
  }

 private:
  void createSampleData() {
    data_ = makeRowVector({
        makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
        makeFlatVector<std::string>({"A", "B", "A", "C", "B", "A", "C", "A", "B", "C"}),
        makeFlatVector<double>({1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.0}),
        makeFlatVector<bool>({true, false, true, true, false, true, false, true, false, true}),
    });
  }

 public:
  void demonstrateBasicSerialization() {
    std::cout << "\n=== Basic Serialization Example ===\n";
    
    // Create a simple plan
    auto plan = exec::test::PlanBuilder()
        .values({data_})
        .project({"c0 * 2 as doubled_id", "c1", "c2 + 1.0 as incremented_value"})
        .filter("doubled_id > 4")
        .planNode();
    
    std::cout << "Original Plan:\n" << plan->toString(true, true) << "\n\n";
    
    // Serialize with default options
    core::PlanNodeJsonSerializer serializer;
    auto result = serializer.serializeToJson(plan);
    
    if (result.isSuccess()) {
      std::cout << "Serialized JSON (compact):\n" << result.value << "\n\n";
    } else {
      std::cout << "Serialization failed: " << result.errors[0].message << "\n";
    }
  }

  void demonstratePrettyPrinting() {
    std::cout << "\n=== Pretty Printing Example ===\n";
    
    auto plan = exec::test::PlanBuilder()
        .values({data_})
        .project({"c0", "c1", "c2"})
        .partialAggregation({"c1"}, {"sum(c0)", "avg(c2)", "count(1)"})
        .finalAggregation()
        .orderBy({"c1 ASC"}, false)
        .planNode();
    
    // Configure pretty printing options
    core::PlanNodeJsonSerializer::SerializationOptions opts;
    opts.prettyPrint = true;
    opts.indentSize = 2;
    opts.includeMetadata = true;
    opts.sortKeys = true;
    
    core::PlanNodeJsonSerializer serializer(opts);
    auto result = serializer.serializeToJson(plan);
    
    if (result.isSuccess()) {
      std::cout << "Pretty-printed JSON with metadata:\n" << result.value << "\n\n";
    } else {
      std::cout << "Serialization failed: " << result.errors[0].message << "\n";
    }
  }

  void demonstrateRoundTrip() {
    std::cout << "\n=== Round-trip Serialization Example ===\n";
    
    // Create a complex plan with joins
    auto planIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    
    auto leftData = makeRowVector({
        makeFlatVector<int32_t>({1, 2, 3, 4}),
        makeFlatVector<std::string>({"A", "B", "C", "D"}),
    });
    
    auto rightData = makeRowVector({
        makeFlatVector<int32_t>({1, 2, 3, 5}),
        makeFlatVector<double>({10.1, 20.2, 30.3, 50.5}),
    });
    
    auto originalPlan = exec::test::PlanBuilder(planIdGenerator)
        .values({leftData})
        .hashJoin(
            {"c0"},
            {"c0"},
            exec::test::PlanBuilder(planIdGenerator)
                .values({rightData})
                .planNode(),
            "",
            {"c0", "c1", "c1_0"},
            core::JoinType::kInner)
        .project({"c0", "c1", "c1_0 * 2.0 as doubled_value"})
        .planNode();
    
    std::cout << "Original Plan Structure:\n" << originalPlan->toString(false, false) << "\n\n";
    
    core::PlanNodeJsonSerializer serializer;
    
    // Serialize
    auto serializeResult = serializer.serializeToJson(originalPlan);
    if (!serializeResult.isSuccess()) {
      std::cout << "Serialization failed: " << serializeResult.errors[0].message << "\n";
      return;
    }
    
    // Deserialize
    auto deserializeResult = serializer.deserializeFromJson(serializeResult.value, pool_.get());
    if (!deserializeResult.isSuccess()) {
      std::cout << "Deserialization failed: " << deserializeResult.errors[0].message << "\n";
      return;
    }
    
    auto deserializedPlan = deserializeResult.value;
    std::cout << "Deserialized Plan Structure:\n" << deserializedPlan->toString(false, false) << "\n\n";
    
    // Compare plans
    auto comparisonResult = serializer.comparePlansViaJson(originalPlan, deserializedPlan);
    if (comparisonResult.isSuccess()) {
      std::cout << "Plans are " << (comparisonResult.value ? "identical" : "different") << "\n\n";
    }
  }

  void demonstrateUtilityFunctions() {
    std::cout << "\n=== Utility Functions Example ===\n";
    
    auto plan = exec::test::PlanBuilder()
        .values({data_})
        .project({"c0", "c1", "c2", "c3"})
        .filter("c3 = true")
        .partialAggregation({"c1"}, {"sum(c0)", "count(1)", "avg(c2)"})
        .finalAggregation()
        .topN({"c1 DESC"}, 5, false)
        .planNode();
    
    // Extract plan node types
    auto nodeTypes = core::extractPlanNodeTypes(plan);
    std::cout << "Plan Node Types: ";
    for (size_t i = 0; i < nodeTypes.size(); ++i) {
      std::cout << nodeTypes[i];
      if (i < nodeTypes.size() - 1) std::cout << ", ";
    }
    std::cout << "\n\n";
    
    // Generate plan summary
    auto summary = core::generatePlanSummary(plan);
    std::cout << "Plan Summary:\n" << folly::toPrettyJson(summary) << "\n\n";
    
    // Pretty print using utility function
    auto prettyJson = core::planNodeToPrettyJson(plan, 4);
    std::cout << "Pretty JSON (using utility):\n" << prettyJson << "\n\n";
    
    // Validate JSON
    bool isValid = core::isValidPlanNodeJson(prettyJson);
    std::cout << "JSON is " << (isValid ? "valid" : "invalid") << "\n\n";
  }

  void demonstrateErrorHandling() {
    std::cout << "\n=== Error Handling Example ===\n";
    
    core::PlanNodeJsonSerializer serializer;
    
    // Test invalid JSON
    auto result1 = serializer.deserializeFromJson("{invalid json}", pool_.get());
    std::cout << "Invalid JSON error: " << 
        (result1.errors.empty() ? "No error details" : result1.errors[0].message) << "\n";
    
    // Test empty JSON
    auto result2 = serializer.deserializeFromJson("", pool_.get());
    std::cout << "Empty JSON error: " << 
        (result2.errors.empty() ? "No error details" : result2.errors[0].message) << "\n";
    
    // Test null plan serialization
    auto result3 = serializer.serializeToJson(nullptr);
    std::cout << "Null plan error: " << 
        (result3.errors.empty() ? "No error details" : result3.errors[0].message) << "\n";
    
    std::cout << "\n";
  }

  void demonstrateMetadataExtraction() {
    std::cout << "\n=== Metadata Extraction Example ===\n";
    
    auto plan = exec::test::PlanBuilder()
        .values({data_})
        .project({"c0", "c1", "c2"})
        .filter("c0 > 3")
        .partialAggregation({"c1"}, {"sum(c0)", "count(1)"})
        .finalAggregation()
        .orderBy({"c1"}, false)
        .limit(0, 5, false)
        .planNode();
    
    core::PlanNodeJsonSerializer serializer;
    auto metadataResult = serializer.extractPlanMetadata(plan);
    
    if (metadataResult.isSuccess()) {
      std::cout << "Plan Metadata:\n" << folly::toPrettyJson(metadataResult.value) << "\n\n";
    } else {
      std::cout << "Metadata extraction failed: " << metadataResult.errors[0].message << "\n";
    }
  }

  void demonstrateSchemaGeneration() {
    std::cout << "\n=== JSON Schema Generation Example ===\n";
    
    auto schema = core::PlanNodeJsonSerializer::generateJsonSchema();
    std::cout << "Generated JSON Schema:\n" << folly::toPrettyJson(schema) << "\n\n";
  }

 private:
  RowVectorPtr data_;
};

int main() {
  try {
    // Initialize memory manager
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    
    PlanNodeJsonExample example;
    
    std::cout << "=== Velox PlanNode JSON Serialization Examples ===\n";
    
    example.demonstrateBasicSerialization();
    example.demonstratePrettyPrinting();
    example.demonstrateRoundTrip();
    example.demonstrateUtilityFunctions();
    example.demonstrateErrorHandling();
    example.demonstrateMetadataExtraction();
    example.demonstrateSchemaGeneration();
    
    std::cout << "=== All examples completed successfully! ===\n";
    
  } catch (const std::exception& e) {
    std::cerr << "Example failed with exception: " << e.what() << std::endl;
    return 1;
  }
  
  return 0;
}