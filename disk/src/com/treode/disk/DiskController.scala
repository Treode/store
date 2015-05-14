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
import com.treode.notify.Notification

/** The disk controller.
  *
  * All the admin-y things you can do with the disk system.
  */
trait DiskController {

  /** This disk system. */
  implicit def disk: Disk

  /** Change the disk drives (or files) that are attached to the system.
    *
    * When this method returns, The newly attached drives will be an active part of the disk
    * system, and the newly draining drives will have begun draining. Draining drives will
    * eventually be detached.
    *
    * @param change The change to apply.
    */
  def change (change: DriveChange): Async [Notification [Unit]]

  /** Summary information. */
  def digest: Async [DiskSystemDigest]

  def shutdown(): Async [Unit]

  /** Attach new drives.
    *
    * When this method returns, the drives are a part of the disk system.
    *
    * @param attaches The drives to attach.
    */
  def attach (attaches: Seq [DriveAttachment]): Async [Notification [Unit]] =
    change (DriveChange (attaches, Seq.empty))

  /** Attach new drives.
    *
    * When this method returns, the drives are a part of the disk system.
    *
    * @param geometry The physical properties of the drives. The same geometry is used for every path.
    * @param paths The path to attach.
    */
  def attach (geometry: DriveGeometry, paths: Path*): Async [Notification [Unit]] =
    attach (paths map (DriveAttachment (_, geometry)))

  /** Drain drives that are attached.
    *
    * The disk system drains drives by copying all live data on them to someplace else. When this
    * method returns, the drain has begun, but it may not complete until later. When the drives
    * have been drained, the disk system will detach them.
    *
    * @param paths The drives to drain.
    */
  def drain (drains: Path*): Async [Notification [Unit]] =
    change (DriveChange (Seq.empty, drains))
}

object DiskController {

  trait Proxy extends DiskController {

    protected def _disk: DiskController

    implicit def disk: Disk =
      _disk.disk

    def digest: Async [DiskSystemDigest] =
      _disk.digest

    def change (change: DriveChange): Async [Notification [Unit]] =
      _disk.change (change)
  }}
