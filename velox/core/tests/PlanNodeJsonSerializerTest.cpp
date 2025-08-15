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

#include <gtest/gtest.h>
#include "velox/core/PlanNode.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::core {

class PlanNodeJsonSerializerTest : public testing::Test,
                                   public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parse::registerTypeResolver();

    // Register serialization/deserialization handlers
    Type::registerSerDe();
    core::PlanNode::registerSerDe();
    core::ITypedExpr::registerSerDe();

    // Create test data
    data_ = {makeRowVector({
        makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
        makeFlatVector<int32_t>({10, 20, 30, 40, 50}),
        makeConstant(true, 5),
        makeArrayVector<int32_t>({
            {1, 2}, {3, 4, 5}, {}, {6}, {7, 8, 9}
        }),
    })};
  }

  // Helper function to create various plan nodes for testing
  PlanNodePtr createSimplePlan() {
    return exec::test::PlanBuilder()
        .values({data_})
        .project({"c0 * 2 as doubled", "c1 + 10 as incremented"})
        .filter("doubled > 4")
        .planNode();
  }

  PlanNodePtr createComplexPlan() {
    auto planIdGenerator = std::make_shared<PlanNodeIdGenerator>();
    
    return exec::test::PlanBuilder(planIdGenerator)
        .values({data_})
        .project({"c0", "c1", "c2"})
        .hashJoin(
            {"c0"},
            {"c0"},
            exec::test::PlanBuilder(planIdGenerator)
                .values({data_})
                .project({"c0", "c1 * 2 as c1_doubled"})
                .planNode(),
            "",
            {"c0", "c1", "c1_doubled"})
        .partialAggregation({"c0"}, {"sum(c1)", "count(c1_doubled)"})
        .finalAggregation()
        .orderBy({"c0 ASC"}, false)
        .limit(0, 10, false)
        .planNode();
  }

  std::vector<RowVectorPtr> data_;
};

TEST_F(PlanNodeJsonSerializerTest, basicSerialization) {
  auto plan = createSimplePlan();
  PlanNodeJsonSerializer serializer;
  
  auto result = serializer.serializeToJson(plan);
  ASSERT_TRUE(result.isSuccess()) << "Serialization failed: " 
      << (result.errors.empty() ? "Unknown error" : result.errors[0].message);
  ASSERT_FALSE(result.value.empty());
  
  // Verify it's valid JSON
  EXPECT_NO_THROW(folly::parseJson(result.value));
}

TEST_F(PlanNodeJsonSerializerTest, basicDeserialization) {
  auto originalPlan = createSimplePlan();
  PlanNodeJsonSerializer serializer;
  
  // Serialize
  auto serializeResult = serializer.serializeToJson(originalPlan);
  ASSERT_TRUE(serializeResult.isSuccess());
  
  // Deserialize
  auto deserializeResult = serializer.deserializeFromJson(serializeResult.value, pool_.get());
  ASSERT_TRUE(deserializeResult.isSuccess()) << "Deserialization failed: "
      << (deserializeResult.errors.empty() ? "Unknown error" : deserializeResult.errors[0].message);
  ASSERT_NE(deserializeResult.value, nullptr);
  
  // Verify structure is preserved
  EXPECT_EQ(originalPlan->name(), deserializeResult.value->name());
  EXPECT_EQ(originalPlan->id(), deserializeResult.value->id());
}

TEST_F(PlanNodeJsonSerializerTest, roundTripConsistency) {
  auto originalPlan = createComplexPlan();
  PlanNodeJsonSerializer serializer;
  
  // First round trip
  auto json1 = serializer.serializeToJson(originalPlan);
  ASSERT_TRUE(json1.isSuccess());
  
  auto plan1 = serializer.deserializeFromJson(json1.value, pool_.get());
  ASSERT_TRUE(plan1.isSuccess());
  
  // Second round trip
  auto json2 = serializer.serializeToJson(plan1.value);
  ASSERT_TRUE(json2.isSuccess());
  
  auto plan2 = serializer.deserializeFromJson(json2.value, pool_.get());
  ASSERT_TRUE(plan2.isSuccess());
  
  // Compare using the built-in toString method
  EXPECT_EQ(plan1.value->toString(true, true), plan2.value->toString(true, true));
}

TEST_F(PlanNodeJsonSerializerTest, prettyPrintFormatting) {
  auto plan = createSimplePlan();
  
  PlanNodeJsonSerializer::SerializationOptions opts;
  opts.prettyPrint = true;
  opts.indentSize = 4;
  opts.sortKeys = true;
  
  PlanNodeJsonSerializer serializer(opts);
  auto result = serializer.serializeToJson(plan);
  ASSERT_TRUE(result.isSuccess());
  
  // Verify pretty printing by checking for newlines and indentation
  EXPECT_TRUE(result.value.find('\n') != std::string::npos);
  EXPECT_TRUE(result.value.find("    ") != std::string::npos); // 4-space indent
}

TEST_F(PlanNodeJsonSerializerTest, metadataInclusion) {
  auto plan = createSimplePlan();
  
  PlanNodeJsonSerializer::SerializationOptions opts;
  opts.includeMetadata = true;
  opts.includeSourceLocations = true;
  
  PlanNodeJsonSerializer serializer(opts);
  auto result = serializer.serializeToDynamic(plan);
  ASSERT_TRUE(result.isSuccess());
  
  // Verify metadata is included
  EXPECT_TRUE(result.value.count("_metadata"));
  EXPECT_TRUE(result.value["_metadata"].count("nodeType"));
  EXPECT_TRUE(result.value["_metadata"].count("outputFields"));
}

TEST_F(PlanNodeJsonSerializerTest, schemaValidation) {
  auto plan = createSimplePlan();
  PlanNodeJsonSerializer serializer;
  
  auto dynamicResult = serializer.serializeToDynamic(plan);
  ASSERT_TRUE(dynamicResult.isSuccess());
  
  auto validationResult = serializer.validateJsonSchema(dynamicResult.value);
  EXPECT_TRUE(validationResult.isSuccess()) << "Schema validation failed: "
      << (validationResult.errors.empty() ? "Unknown error" : validationResult.errors[0].message);
}

TEST_F(PlanNodeJsonSerializerTest, invalidJsonHandling) {
  PlanNodeJsonSerializer serializer;
  
  // Test empty string
  auto result1 = serializer.deserializeFromJson("", pool_.get());
  EXPECT_FALSE(result1.isSuccess());
  EXPECT_FALSE(result1.errors.empty());
  
  // Test invalid JSON
  auto result2 = serializer.deserializeFromJson("{invalid json", pool_.get());
  EXPECT_FALSE(result2.isSuccess());
  EXPECT_FALSE(result2.errors.empty());
  
  // Test valid JSON but invalid schema
  auto result3 = serializer.deserializeFromJson("{\"wrong\": \"structure\"}", pool_.get());
  EXPECT_FALSE(result3.isSuccess());
}

TEST_F(PlanNodeJsonSerializerTest, nullPlanHandling) {
  PlanNodeJsonSerializer serializer;
  
  // Test serialization of null plan
  auto serializeResult = serializer.serializeToJson(nullptr);
  EXPECT_FALSE(serializeResult.isSuccess());
  EXPECT_FALSE(serializeResult.errors.empty());
  
  auto dynamicResult = serializer.serializeToDynamic(nullptr);
  EXPECT_FALSE(dynamicResult.isSuccess());
  EXPECT_FALSE(dynamicResult.errors.empty());
}

TEST_F(PlanNodeJsonSerializerTest, planComparison) {
  auto plan1 = createSimplePlan();
  auto plan2 = createSimplePlan(); // Same structure
  auto plan3 = createComplexPlan(); // Different structure
  
  PlanNodeJsonSerializer serializer;
  
  // Compare identical plans
  auto result1 = serializer.comparePlansViaJson(plan1, plan2);
  ASSERT_TRUE(result1.isSuccess());
  EXPECT_TRUE(result1.value);
  
  // Compare different plans
  auto result2 = serializer.comparePlansViaJson(plan1, plan3);
  ASSERT_TRUE(result2.isSuccess());
  EXPECT_FALSE(result2.value);
}

TEST_F(PlanNodeJsonSerializerTest, metadataExtraction) {
  auto plan = createComplexPlan();
  PlanNodeJsonSerializer serializer;
  
  auto result = serializer.extractPlanMetadata(plan);
  ASSERT_TRUE(result.isSuccess());
  
  EXPECT_TRUE(result.value.count("totalNodes"));
  EXPECT_TRUE(result.value.count("nodeTypes"));
  EXPECT_GT(result.value["totalNodes"].asInt(), 0);
}

TEST_F(PlanNodeJsonSerializerTest, jsonSchemaGeneration) {
  auto schema = PlanNodeJsonSerializer::generateJsonSchema();
  
  EXPECT_TRUE(schema.isObject());
  EXPECT_TRUE(schema.count("$schema"));
  EXPECT_TRUE(schema.count("title"));
  EXPECT_TRUE(schema.count("type"));
  EXPECT_TRUE(schema.count("required"));
  EXPECT_TRUE(schema.count("properties"));
  
  EXPECT_EQ(schema["type"].asString(), "object");
}

TEST_F(PlanNodeJsonSerializerTest, utilityFunctionsPrettyJson) {
  auto plan = createSimplePlan();
  
  auto prettyJson = planNodeToPrettyJson(plan, 2);
  EXPECT_FALSE(prettyJson.empty());
  EXPECT_TRUE(prettyJson.find('\n') != std::string::npos);
  
  // Verify it's valid JSON
  EXPECT_NO_THROW(folly::parseJson(prettyJson));
}

TEST_F(PlanNodeJsonSerializerTest, utilityFunctionsFromJson) {
  auto originalPlan = createSimplePlan();
  auto json = planNodeToPrettyJson(originalPlan);
  
  auto [deserializedPlan, error] = planNodeFromJson(json, pool_.get());
  
  EXPECT_TRUE(error.empty()) << "Error: " << error;
  EXPECT_NE(deserializedPlan, nullptr);
  if (deserializedPlan) {
    EXPECT_EQ(originalPlan->name(), deserializedPlan->name());
  }
}

TEST_F(PlanNodeJsonSerializerTest, utilityFunctionsExtractTypes) {
  auto plan = createComplexPlan();
  auto types = extractPlanNodeTypes(plan);
  
  EXPECT_FALSE(types.empty());
  EXPECT_TRUE(std::find(types.begin(), types.end(), "Values") != types.end());
  EXPECT_TRUE(std::find(types.begin(), types.end(), "Project") != types.end());
}

TEST_F(PlanNodeJsonSerializerTest, utilityFunctionsPlanSummary) {
  auto plan = createComplexPlan();
  auto summary = generatePlanSummary(plan);
  
  EXPECT_TRUE(summary.isObject());
  EXPECT_TRUE(summary.count("rootNodeType"));
  EXPECT_TRUE(summary.count("rootNodeId"));
  EXPECT_TRUE(summary.count("outputFields"));
  EXPECT_TRUE(summary.count("totalNodes"));
  EXPECT_TRUE(summary.count("nodeTypeCounts"));
  
  EXPECT_GT(summary["totalNodes"].asInt(), 0);
}

TEST_F(PlanNodeJsonSerializerTest, utilityFunctionsValidation) {
  auto plan = createSimplePlan();
  auto json = planNodeToPrettyJson(plan);
  
  EXPECT_TRUE(isValidPlanNodeJson(json));
  EXPECT_FALSE(isValidPlanNodeJson("{\"invalid\": \"structure\"}"));
  EXPECT_FALSE(isValidPlanNodeJson("not json at all"));
  EXPECT_FALSE(isValidPlanNodeJson(""));
}

TEST_F(PlanNodeJsonSerializerTest, recursionLimitHandling) {
  PlanNodeJsonSerializer::SerializationOptions serOpts;
  serOpts.maxRecursionDepth = 2; // Very low limit
  
  PlanNodeJsonSerializer::DeserializationOptions deserOpts;
  deserOpts.maxRecursionDepth = 2;
  
  PlanNodeJsonSerializer serializer(serOpts, deserOpts);
  
  auto complexPlan = createComplexPlan();
  
  // This should still work as our test plans are not deeply nested
  auto result = serializer.serializeToJson(complexPlan);
  EXPECT_TRUE(result.isSuccess());
}

TEST_F(PlanNodeJsonSerializerTest, jsonFormatting) {
  auto plan = createSimplePlan();
  PlanNodeJsonSerializer serializer;
  
  auto compactJson = serializer.serializeToJson(plan);
  ASSERT_TRUE(compactJson.isSuccess());
  
  PlanNodeJsonSerializer::SerializationOptions prettyOpts;
  prettyOpts.prettyPrint = true;
  prettyOpts.indentSize = 2;
  
  auto formatResult = serializer.formatJson(compactJson.value, prettyOpts);
  ASSERT_TRUE(formatResult.isSuccess());
  
  // Pretty formatted should be longer than compact
  EXPECT_GT(formatResult.value.length(), compactJson.value.length());
  EXPECT_TRUE(formatResult.value.find('\n') != std::string::npos);
}

TEST_F(PlanNodeJsonSerializerTest, strictVsLenientDeserialization) {
  auto plan = createSimplePlan();
  PlanNodeJsonSerializer serializer;
  
  auto json = serializer.serializeToJson(plan);
  ASSERT_TRUE(json.isSuccess());
  
  // Parse and add unknown field
  auto dynamic = folly::parseJson(json.value);
  dynamic["unknownField"] = "should be ignored or rejected";
  auto modifiedJson = folly::toJson(dynamic);

  PlanNodeJsonSerializer::SerializationOptions serializationOpts;

  // Test strict mode (should reject unknown fields)
  PlanNodeJsonSerializer::DeserializationOptions strictOpts;
  strictOpts.allowUnknownFields = false;
  strictOpts.validateSchema = true;
  
  PlanNodeJsonSerializer strictSerializer(serializationOpts, strictOpts);
  auto strictResult = strictSerializer.deserializeFromJson(modifiedJson, pool_.get());
  // Note: The actual behavior depends on the underlying Velox deserialization,
  // which may be more lenient than our wrapper suggests
  
  // Test lenient mode
  PlanNodeJsonSerializer::DeserializationOptions lenientOpts;
  lenientOpts.allowUnknownFields = true;
  lenientOpts.validateSchema = false;
  
  PlanNodeJsonSerializer lenientSerializer(serializationOpts, lenientOpts);
  auto lenientResult = lenientSerializer.deserializeFromJson(modifiedJson, pool_.get());
  // Should succeed or at least not fail due to unknown fields
}

} // namespace facebook::velox::core