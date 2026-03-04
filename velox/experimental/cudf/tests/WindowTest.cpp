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
#include "velox/experimental/cudf/exec/CudfConversion.h"
#include "velox/experimental/cudf/exec/ToCudf.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace {

class CudfWindowTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    window::prestosql::registerAllWindowFunctions();
    filesystems::registerLocalFileSystem();
    cudf_velox::registerCudf();
  }

  void TearDown() override {
    cudf_velox::unregisterCudf();
    OperatorTestBase::TearDown();
  }
};

// ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...): verifies CudfWindow and
// CudfToVelox produce correct output type (BIGINT for row_number) and names.
TEST_F(CudfWindowTest, rowNumberPartitionOrder) {
  auto data = makeRowVector(
      {"id", "val"},
      {
          makeFlatVector<int32_t>({1, 1, 1, 2, 2, 2}),
          makeFlatVector<int64_t>({10, 20, 30, 15, 25, 35}),
      });
  createDuckDbTable({data});

  auto plan = PlanBuilder()
                  .values({data})
                  .window({"row_number() over (partition by id order by val)"})
                  .orderBy({"id ASC NULLS LAST", "val ASC NULLS LAST"}, false)
                  .planNode();

  assertQueryOrdered(
      plan,
      "SELECT id, val, ROW_NUMBER() OVER (PARTITION BY id ORDER BY val) as row_number FROM tmp ORDER BY id, val",
      {0, 1});
}

// LAG/LEAD: window functions that produce same-type column; output schema
// must match plan (correct names and types from CudfToVelox).
TEST_F(CudfWindowTest, lagLead) {
  auto data = makeRowVector(
      {"id", "val"},
      {
          makeFlatVector<int32_t>({1, 1, 1, 2, 2}),
          makeFlatVector<int64_t>({100, 200, 300, 10, 20}),
      });
  createDuckDbTable({data});

  auto plan = PlanBuilder()
                  .values({data})
                  .window({
                      "lag(val, 1) over (partition by id order by val) as lag_val",
                      "lead(val, 1) over (partition by id order by val) as lead_val",
                  })
                  .orderBy({"id ASC NULLS LAST", "val ASC NULLS LAST"}, false)
                  .planNode();

  assertQueryOrdered(
      plan,
      "SELECT id, val, "
      "LAG(val, 1) OVER (PARTITION BY id ORDER BY val) as lag_val, "
      "LEAD(val, 1) OVER (PARTITION BY id ORDER BY val) as lead_val "
      "FROM tmp ORDER BY id, val",
      {0, 1});
}

} // namespace
