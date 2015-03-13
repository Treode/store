/*
 * Copyright 2014 Treode, Inc.
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

package com.treode.disk

object DiskTestConfig {

  def apply (
      checkpointBytes: Int = Int.MaxValue,
      checkpointEntries: Int = Int.MaxValue,
      cleaningFrequency: Int = Int.MaxValue,
      cleaningLoad: Int = 1,
      maximumRecordBytes: Int = 1<<10,
      maximumPageBytes: Int = 1<<10,
      pageCacheEntries: Int = 100,
      superBlockBits: Int = 8
  ): DiskConfig =
    DiskConfig (
        checkpointBytes = checkpointBytes,
        checkpointEntries = checkpointEntries,
        cleaningFrequency = cleaningFrequency,
        cleaningLoad = cleaningLoad,
        maximumRecordBytes = maximumRecordBytes,
        maximumPageBytes = maximumPageBytes,
        pageCacheEntries = pageCacheEntries,
        superBlockBits = superBlockBits)
}
