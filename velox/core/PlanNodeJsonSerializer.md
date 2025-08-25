# PlanNode JSON Serialization and Deserialization

This module provides enhanced JSON serialization and deserialization capabilities for Velox PlanNode structures using Facebook's folly library. It builds upon the existing Velox serialization infrastructure to provide additional features like pretty printing, schema validation, and enhanced error handling.

## Overview

The `PlanNodeJsonSerializer` class provides a comprehensive interface for converting PlanNode objects to and from JSON format. It supports various configuration options and utility functions to handle different use cases.

## Features

- **JSON Serialization**: Convert PlanNode structures to JSON strings or folly::dynamic objects
- **JSON Deserialization**: Parse JSON back into PlanNode structures with validation
- **Pretty Printing**: Format JSON with customizable indentation and sorting
- **Schema Validation**: Validate JSON structure against expected PlanNode schema
- **Metadata Extraction**: Generate statistics and metadata about plan structures
- **Error Handling**: Comprehensive error reporting with context information
- **Utility Functions**: Convenient helper functions for common operations

## Basic Usage

### Simple Serialization and Deserialization

```cpp
#include "velox/core/PlanNodeJsonSerializer.h"

// Create a plan node (using PlanBuilder)
auto plan = PlanBuilder()
    .values({data})
    .project({"c0 * 2 as doubled", "c1"})
    .filter("doubled > 4")
    .planNode();

// Create serializer with default options
core::PlanNodeJsonSerializer serializer;

// Serialize to JSON
auto serializeResult = serializer.serializeToJson(plan);
if (serializeResult.isSuccess()) {
    std::string json = serializeResult.value;
    std::cout << "JSON: " << json << std::endl;
}

// Deserialize from JSON
auto deserializeResult = serializer.deserializeFromJson(json, pool);
if (deserializeResult.isSuccess()) {
    auto deserializedPlan = deserializeResult.value;
    // Use the deserialized plan...
}
```

### Pretty Printing

```cpp
// Configure pretty printing options
core::PlanNodeJsonSerializer::SerializationOptions opts;
opts.prettyPrint = true;
opts.indentSize = 2;
opts.includeMetadata = true;
opts.sortKeys = true;

core::PlanNodeJsonSerializer serializer(opts);
auto result = serializer.serializeToJson(plan);

if (result.isSuccess()) {
    std::cout << "Pretty JSON:\n" << result.value << std::endl;
}
```

### Error Handling

```cpp
auto result = serializer.deserializeFromJson(invalidJson, pool);
if (!result.isSuccess()) {
    for (const auto& error : result.errors) {
        std::cout << "Error: " << error.message << std::endl;
        if (!error.context.empty()) {
            std::cout << "Context: " << error.context << std::endl;
        }
    }
}
```

## Configuration Options

### SerializationOptions

- `prettyPrint`: Enable pretty printing with indentation (default: false)
- `indentSize`: Number of spaces for indentation (default: 2)
- `includeMetadata`: Include additional metadata in output (default: false)
- `sortKeys`: Sort JSON keys alphabetically (default: true)
- `includeSourceLocations`: Include debugging information (default: false)
- `maxRecursionDepth`: Maximum depth to prevent infinite loops (default: 100)

### DeserializationOptions

- `validateSchema`: Validate JSON against expected schema (default: true)
- `allowUnknownFields`: Allow unknown fields in JSON (default: false)
- `strictTypeChecking`: Use strict type validation (default: true)
- `maxRecursionDepth`: Maximum depth to prevent infinite loops (default: 100)

## Utility Functions

The module provides several utility functions for common operations:

```cpp
// Convert to pretty-printed JSON
std::string json = core::planNodeToPrettyJson(plan, 4);

// Deserialize with error handling
auto [plan, error] = core::planNodeFromJson(json, pool);

// Extract node types from plan
std::vector<std::string> types = core::extractPlanNodeTypes(plan);

// Generate plan summary
folly::dynamic summary = core::generatePlanSummary(plan);

// Validate JSON structure
bool isValid = core::isValidPlanNodeJson(json);
```

## Advanced Features

### Schema Validation

```cpp
// Generate JSON schema for validation
auto schema = core::PlanNodeJsonSerializer::generateJsonSchema();

// Validate against schema
auto validationResult = serializer.validateJsonSchema(dynamicJson);
if (!validationResult.isSuccess()) {
    // Handle validation errors...
}
```

### Plan Comparison

```cpp
// Compare two plans via JSON serialization
auto comparisonResult = serializer.comparePlansViaJson(plan1, plan2);
if (comparisonResult.isSuccess()) {
    bool areEqual = comparisonResult.value;
    std::cout << "Plans are " << (areEqual ? "equal" : "different") << std::endl;
}
```

### Metadata Extraction

```cpp
// Extract plan statistics and metadata
auto metadataResult = serializer.extractPlanMetadata(plan);
if (metadataResult.isSuccess()) {
    folly::dynamic metadata = metadataResult.value;
    int totalNodes = metadata["totalNodes"].asInt();
    // Process metadata...
}
```

### JSON Formatting

```cpp
// Reformat existing JSON with different options
core::PlanNodeJsonSerializer::SerializationOptions newOpts;
newOpts.prettyPrint = true;
newOpts.indentSize = 4;

auto formatResult = serializer.formatJson(compactJson, newOpts);
if (formatResult.isSuccess()) {
    std::string formattedJson = formatResult.value;
}
```

## Integration with Existing Velox Code

This serializer builds on top of the existing Velox serialization infrastructure and is compatible with the current `PlanNode::serialize()` and `ISerializable::deserialize<PlanNode>()` methods. It provides additional functionality while maintaining compatibility.

### Required Registrations

Before using the serializer, ensure the following registrations are completed:

```cpp
// Register scalar and aggregate functions
functions::prestosql::registerAllScalarFunctions();
aggregate::prestosql::registerAllAggregateFunctions();
parse::registerTypeResolver();

// Register serialization handlers
Type::registerSerDe();
core::PlanNode::registerSerDe();
core::ITypedExpr::registerSerDe();
```

## Testing

The module includes comprehensive unit tests covering:

- Basic serialization and deserialization
- Round-trip consistency
- Pretty printing and formatting
- Error handling scenarios
- Schema validation
- Utility function behavior
- Edge cases and error conditions

Run tests with:
```bash
# Build and run tests
make velox_core_test
./velox_core_test --gtest_filter="PlanNodeJsonSerializerTest.*"
```

## Examples

See `PlanNodeJsonSerializerExample.cpp` for comprehensive examples demonstrating all features of the JSON serializer.

## Performance Considerations

- The serializer uses the existing Velox serialization infrastructure, so performance characteristics are similar to the base implementation
- Pretty printing adds formatting overhead but is primarily intended for debugging and human-readable output
- Schema validation adds computational overhead but can be disabled for performance-critical paths
- Metadata extraction involves traversing the entire plan tree

## Limitations

- Serialization depends on the underlying Velox PlanNode serialization support
- Some advanced plan node types may require additional serialization support
- JSON format is primarily intended for debugging, testing, and plan exchange rather than high-performance scenarios
- Deep plan structures may hit recursion limits (configurable)

## Contributing

When adding new PlanNode types or modifying existing ones:

1. Ensure the new types support the existing Velox serialization interface
2. Add test cases covering the new functionality
3. Update documentation and examples as needed
4. Consider schema validation requirements for new fields

## Dependencies

- folly library for JSON operations
- Velox core libraries
- Existing Velox serialization infrastructure
- Google Test framework for testing