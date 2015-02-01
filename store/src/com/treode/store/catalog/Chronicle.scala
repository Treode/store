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

package com.treode.store.catalog

import java.util.ArrayDeque
import scala.collection.JavaConversions._

import com.nothome.delta.{Delta, GDiffPatcher}
import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.misc.materialize
import com.treode.disk.{Disk, PageDescriptor, Position, RecordDescriptor}
import com.treode.store.{Bytes, CatalogId, StaleException}

import Async.{guard, when}
import Callback.ignore
import Handler.{Meta, pager}

private class Chronicle (
    var version: Int,
    var checksum: Int,
    var bytes:  Bytes,
    var history: ArrayDeque [Bytes]
) {

  def diff (other: Int): Update = {
    val start = other - version + history.size
    if (start >= history.size) {
      Patch (version, checksum, Seq.empty)
    } else if (start < 0) {
      Assign (version, bytes, history.toSeq)
    } else {
      Patch (version, checksum, history.drop (start) .toSeq)
    }
  }

  def diff (version: Int, bytes: Bytes): Patch = {
    if (version != this.version + 1)
      throw new StaleException
    Patch (version, bytes.murmur32, Seq (Patch.diff (this.bytes, bytes)))
  }

  def patch (version: Int, bytes: Bytes, history: Seq [Bytes]): Option [Update] = {
    if (version <= this.version)
      return None
    this.version = version
    this.checksum = bytes.murmur32
    this.bytes = bytes
    this.history.clear()
    this.history.addAll (history)
    Some (Assign (version, bytes, history))
  }

  def patch (end: Int, checksum: Int, patches: Seq [Bytes]) : Option [Update] = {
    val span = end - version
    if (span <= 0 || patches.length < span)
      return None
    val future = patches drop (patches.length - span)
    var bytes = this.bytes
    for (patch <- future)
      bytes = Patch.patch (bytes, patch)
    assert (bytes.murmur32 == checksum, "Patch application went awry.")
    this.version += span
    this.checksum = checksum
    this.bytes = bytes
    for (_ <- 0 until history.size + span - catalogHistoryLimit)
      history.remove()
    history.addAll (future)
    Some (Patch (version, checksum, future))
  }
}

private object Chronicle {
  def apply : Chronicle =
    new Chronicle (0, Bytes.empty.murmur32, Bytes.empty, new ArrayDeque)
}
