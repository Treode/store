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

/** Attach a disk drive (or file) to the system.
  *
  * @param path The path to the raw device (or file).
  * @param geometry The physical properties of the drive.
  */
case class DriveAttachment (path: Path, geometry: DriveGeometry)

object DriveAttachment {

  def fromJson (node: JsonReader): Notification [DriveAttachment] =
    for {
      obj <- node.requireObject
      (path, geometry) <- Notification.latch (
        node.getFilePath ("path"),
        node.readObject ("geometry") (DriveGeometry.fromJson _))
    } yield {
      DriveAttachment (path, geometry)
    }}
