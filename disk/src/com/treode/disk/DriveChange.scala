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

import com.treode.jackson.JsonReader
import com.treode.notify.Notification

/** Change the disk drives (or files) that are attached to the system.
  *
  * @param attaches The drives to attach. They must not be attached already.
  * @param drains The drives to drain. They must be attached currently. The disk system will
  * begin draining them, that is copying their live data to other disks, and then it will
  * eventually detach them.
  */
case class DriveChange (attaches: Seq [DriveAttachment], drains: Seq [Path])

object DriveChange {

  val empty = DriveChange (Seq.empty, Seq.empty)

  def fromJson (node: JsonReader): Notification [DriveChange] =
    for {
      obj <- node.requireObject
      (attaches, drains) <- Notification.latch (
        node.readArray ("attach") (DriveAttachment.fromJson _),
        node.readArray ("drain") (_.getFilePath()))
    } yield {
      DriveChange (attaches, drains)
    }}
