# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include_guard(GLOBAL)

# cuGeo's Velox adapter, built against Velox's cuDF integration.
#
# cuGeo commit eb0316d79 from 2026-11-10 (branch simoneves/cugeo-presto-plugin)
set(VELOX_cugeo_COMMIT eb0316d79bbb44521275bd8330ca3b45d3e82399)
# cuGeo is private on GitLab for now, so it is cloned over SSH with the caller's
# SSH agent, on NVIDIA's network. When cuGeo is public on GitHub, replace this
# URL with its https:// clone URL; nothing else here depends on SSH.
set(VELOX_cugeo_SOURCE_URL "ssh://git@gitlab-master.nvidia.com:12051/geonemo/cugeo.git")
velox_set_with_default(VELOX_cugeo_SOURCE_URL VELOX_cugeo_URL ${VELOX_cugeo_SOURCE_URL})

# Velox needs only this part of the cuGeo repository. The clone is sparse and
# partial, so the other parts are neither checked out nor downloaded.
set(VELOX_cugeo_SPARSE_PATH cugeo-geometry)
set(VELOX_cugeo_SOURCE_SUBDIR cugeo-geometry/backends/cugeo-velox)

# Runs git in the cuGeo checkout and fails the configure on any error.
function(velox_cugeo_git source_dir)
  execute_process(
    COMMAND git -C "${source_dir}" ${ARGN}
    RESULT_VARIABLE result
    OUTPUT_VARIABLE output
    ERROR_VARIABLE error
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  if(NOT result EQUAL 0)
    message(FATAL_ERROR "git ${ARGN} failed in ${source_dir}:\n${error}")
  endif()
  set(git_output "${output}" PARENT_SCOPE)
endfunction()

# Checks out VELOX_cugeo_SPARSE_PATH of VELOX_cugeo_COMMIT into source_dir.
# The commit may be on any branch of the repository; a later configure with
# another commit fetches it into the same checkout.
function(velox_cugeo_checkout source_dir)
  if(NOT EXISTS "${source_dir}/.git")
    file(REMOVE_RECURSE "${source_dir}")
    message(STATUS "Cloning cuGeo from ${VELOX_cugeo_SOURCE_URL}")
    execute_process(
      COMMAND
        git clone --quiet --filter=blob:none --no-checkout --sparse "${VELOX_cugeo_SOURCE_URL}"
        "${source_dir}"
      RESULT_VARIABLE result
      ERROR_VARIABLE error
    )
    if(NOT result EQUAL 0)
      message(FATAL_ERROR "Cloning cuGeo from ${VELOX_cugeo_SOURCE_URL} failed:\n${error}")
    endif()
  endif()
  velox_cugeo_git("${source_dir}" sparse-checkout set "${VELOX_cugeo_SPARSE_PATH}")
  execute_process(
    COMMAND git -C "${source_dir}" cat-file -e "${VELOX_cugeo_COMMIT}^{commit}"
    RESULT_VARIABLE have_commit
    OUTPUT_QUIET
    ERROR_QUIET
  )
  if(NOT have_commit EQUAL 0)
    velox_cugeo_git("${source_dir}" fetch --quiet --filter=blob:none origin)
  endif()
  velox_cugeo_git("${source_dir}" checkout --quiet --detach "${VELOX_cugeo_COMMIT}")
  velox_cugeo_git("${source_dir}" rev-parse HEAD)
  if(NOT git_output STREQUAL VELOX_cugeo_COMMIT)
    message(FATAL_ERROR "cuGeo checkout is at ${git_output}, expected ${VELOX_cugeo_COMMIT}")
  endif()
endfunction()

if(NOT VELOX_ENABLE_CUDF)
  message(FATAL_ERROR "cuGeo requires VELOX_ENABLE_CUDF=ON.")
endif()

include(FetchContent)
if(FETCHCONTENT_SOURCE_DIR_CUGEO)
  # A local cuGeo clone, used as it is.
  set(cugeo_source_dir "${FETCHCONTENT_SOURCE_DIR_CUGEO}")
  message(STATUS "Using local cuGeo at ${cugeo_source_dir}")
else()
  set(cugeo_source_dir "${FETCHCONTENT_BASE_DIR}/cugeo-src")
  velox_cugeo_checkout("${cugeo_source_dir}")
  message(STATUS "Using cuGeo ${VELOX_cugeo_COMMIT} at ${cugeo_source_dir}")
endif()

FetchContent_Declare(
  cugeo
  SOURCE_DIR "${cugeo_source_dir}"
  SOURCE_SUBDIR ${VELOX_cugeo_SOURCE_SUBDIR}
)
FetchContent_MakeAvailable(cugeo)
