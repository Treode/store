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

package com.treode

import java.nio.file.Path
import java.util.{ArrayList, HashMap}

import com.treode.async.{Async, Scheduler}
import com.treode.buffer.PagedBuffer
import org.apache.commons.lang3.StringEscapeUtils.escapeJava

package disk {

  case class Compaction (obj: ObjectId, gens: Set [Long])

  case class DiskSystemDigest (drives: Seq [DriveDigest])

  case class DriveChange (attaches: Seq [DriveAttachment], drains: Seq [Path])

  case class DriveDigest (path: Path, geometry: DriveGeometry, allocated: Int, draining: Boolean)

  case class ReattachFailure (path: Path, thrown: Throwable) {
    override def toString = s"Could not reattach ${quote (path)}: $thrown"
  }

  class DisksClosedException extends IllegalStateException {
    override def getMessage = "The disk system is closed."
  }

  class DiskFullException extends Exception {
    override def getMessage = "Disk full."
  }

  class OversizedPageException (maximum: Int, found: Int) extends IllegalArgumentException {
    override def getMessage = s"The page of $found bytes exceeds the limit of $maximum bytes."
  }

  class OversizedRecordException (maximum: Int, found: Int) extends IllegalArgumentException {
    override def getMessage = s"The record of $found bytes exceeds the limit of $maximum bytes."
  }

  class ReattachException (failures: Seq [ReattachFailure]) extends Exception {
    override def getMessage() = failures mkString "; "
  }}

package object disk {

  private [disk] type Checkpoints = ArrayList [Unit => Async [Unit]]

  private [disk] type Compactors = HashMap [TypeId, Compaction => Async [Unit]]

  private [disk] def quote (path: Path): String =
    "\"" + escapeJava (path.toString) + "\""

  private [disk] def quote (paths: Iterable [Path]): String =
    paths map (quote _) mkString ", "
}
