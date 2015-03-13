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

/** The launch builder. */
trait DiskLaunch {

  /** The disk system.
    *
    * It is ready for recording log entries, and reading and writing pages. However, before
    * launch it is not checkpointing or cleaning pages. It is not reclaiming segments.
    */
  implicit def disk: Disk

  /** The disk controller. */
  implicit def controller: DiskController

  /** The SystemId found in the superblock. */
  def sysid: SystemId

  /** Register a checkpointer. */
  def checkpoint (f: => Async [Unit])

  /** Register a page handler. */
  def handle (desc: PageDescriptor [_], handler: PageHandler)

  /** Launch the checkpointer and cleaner.
    *
    * Call `launch` after registering all checkpointers and page handlers. This method closes
    * this launch builder.
    */
  def launch()
}
