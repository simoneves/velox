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

# Adds the out-of-tree projects listed in VELOX_CUDF_EXTENSIONS to the build.
#
# Each entry has the form <name>=<location>[|<subdir>]. <location> is either a
# local source directory or <url>#<sha256> of an archive. <subdir> names the
# directory inside the source that holds the project's CMakeLists.txt. Each
# project is added with FetchContent, so its build tree is _deps/<name>-build,
# and it can link Velox's targets, including the cuDF ones, directly.
function(velox_add_cudf_extensions)
  if(NOT VELOX_CUDF_EXTENSIONS)
    return()
  endif()
  if(NOT VELOX_ENABLE_CUDF)
    message(FATAL_ERROR "VELOX_CUDF_EXTENSIONS requires VELOX_ENABLE_CUDF=ON.")
  endif()
  include(FetchContent)
  foreach(entry IN LISTS VELOX_CUDF_EXTENSIONS)
    if(NOT entry MATCHES "^([A-Za-z0-9_]+)=([^|]+)(\\|(.+))?$")
      message(
        FATAL_ERROR
        "Invalid VELOX_CUDF_EXTENSIONS entry, expected <name>=<location>[|<subdir>]: ${entry}"
      )
    endif()
    set(name "${CMAKE_MATCH_1}")
    set(location "${CMAKE_MATCH_2}")
    set(subdir "${CMAKE_MATCH_4}")
    if(IS_DIRECTORY "${location}")
      set(source_args SOURCE_DIR "${location}")
    elseif(location MATCHES "^(.+)#([0-9A-Fa-f]{64})$")
      set(source_args URL "${CMAKE_MATCH_1}" URL_HASH "SHA256=${CMAKE_MATCH_2}")
    else()
      message(
        FATAL_ERROR
        "VELOX_CUDF_EXTENSIONS location is neither a directory nor <url>#<sha256>: ${location}"
      )
    endif()
    if(subdir)
      list(APPEND source_args SOURCE_SUBDIR "${subdir}")
    endif()
    message(STATUS "Adding cuDF extension ${name} from ${location}")
    FetchContent_Declare(${name} ${source_args})
    FetchContent_MakeAvailable(${name})
  endforeach()
endfunction()
