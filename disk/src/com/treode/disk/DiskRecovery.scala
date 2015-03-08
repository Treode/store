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

  /** Register a replayer for a log entry. */
  def replay [R] (desc: RecordDescriptor [R]) (f: R => Any)

  /** Reattach one or more paths.
    *
    * You need provide only some of the paths previously attached to this disk system. The
    * recovery mechanim will find the complete list of paths in the superblock. If not for this
    * behavior, you would need to attach and drain disks, and simultaneously update the config
    * files or startup scripts. With this feature, you can
    *
    *   - Add a disk:
    *
    *       1. Attach the disk; see [[DiskController#attach]].
    *
    *       2. Add this disk to config files or startup scripts.
    *
    *   - Drain a disk.
    *
    *       1. Remove the disk from config files or startup scripts.
    *
    *       2. Drain the disk; see [[DiskController#drain]].
    *
    * Call `reattach` ''after'' registering all replayers. This method closes this recovery
    * builder.
    */
  def reattach (items: Path*): Async [DiskLaunch]
}
