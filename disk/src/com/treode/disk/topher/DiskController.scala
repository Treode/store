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

/** The disk controller.
  *
  * All the admin-y things you can do with the disk system.
  */
trait DiskController {

  /** This disk system. */
  implicit def disk: Disk

  /** Summary information of the drives attached to this disk system. */
  def drives: Async [Seq [DriveDigest]]

  /** Attach new drives.
    *
    * When this method returns, the drives are a part of the disk system.
    */
  def attach (items: DriveAttachment*): Async [Unit]

  /** Drain attached drives.
    *
    * The disk system drains drives by copying all live data on them to someplace else. When this
    * method returns, the drain has begun, but it may not complete until later. When they have
    * drained, the disk system will detach the drives and log a message.
    */
  def drain (items: Path*): Async [Unit]

  def shutdown(): Async [Unit]
}
