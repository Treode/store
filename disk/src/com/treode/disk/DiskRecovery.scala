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

import java.nio.file.Path

import com.treode.async.Async

/** The recovery builder. */
trait DiskRecovery {

  /** Register a method to replay a log entry.
    *
    * A replayer must be registered for every type of log entry that may be in the write log. The
    * disk system will refuse to recover if it cannot identify all log entries.
    */
  def replay [R] (desc: RecordDescriptor [R]) (f: R => Any)

  /** Reattach one or more disk drives.
    *
    * You need provide only some of the paths previously attached to this disk system. The
    * recovery mechanim will find the complete list of paths in the superblock.
    *
    * Call `reattach` after registering all replayers. This method closes the recovery builder.
    */
  def reattach (items: Path*): Async [DiskLaunch]
}
