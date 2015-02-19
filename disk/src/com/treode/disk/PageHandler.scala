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

import com.treode.async.Async

/** Determines which pages are live, and compacts pages. */
trait PageHandler {

  /** Returns those groups which are still live.
    *
    * The disk system will provide a set of group IDs. These are the IDs from the time the pages
    * were written. They can be anything, as long as they allow the handler to positiviley
    * identify which groups are still live.
    */
  def probe (obj: ObjectId, groups: Set [GroupId]): Async [Set [GroupId]]

  /** Compact the object.
    *
    * The disk system will provide a set of group IDs that '''MUST''' be compacted; pages for those
    * groups will be reclaimed and overwritten after compaction completes. The handler may copy
    * other groups as well. These are the IDs from the time the pages were written. They can be
    * anything, as long as they allow the object to positiviley identify which groups must be
    * copied.
    */
  def compact (obj: ObjectId, groups: Set [GroupId]): Async [Unit]
}
