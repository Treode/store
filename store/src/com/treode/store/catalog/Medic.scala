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

import com.treode.async.Async
import com.treode.disk.Disk
import com.treode.store.{Bytes, CatalogId}

import Async.{guard, when}
import Handler.{Meta, pager}

private class Medic (id: CatalogId) {

  var version = 0
  var bytes = Bytes.empty
  var history = new ArrayDeque [Bytes]
  var saved = Option.empty [Meta]

  def patch (version: Int, bytes: Bytes, history: Seq [Bytes]) {
    if (this.version < version) {
      this.version = version
      this.bytes = bytes
      this.history.clear()
      this.history.addAll (history)
    } else {
      val start = version - history.size
      val thisStart = version - history.size
      if (start < thisStart && this.history.size < catalogHistoryLimit) {
        for (patch <- history take (thisStart - start))
          this.history.push (patch)
      }}}

  def patch (end: Int, patches: Seq [Bytes]) {
    val span = end - version
    if (0 < span && span <= patches.length) {
      val future = patches drop (patches.length - end + version)
      var bytes = this.bytes
      for (patch <- future)
        bytes = Patch.patch (bytes, patch)
      this.version += span
      this.bytes = bytes
      for (_ <- 0 until history.size + span - catalogHistoryLimit)
        history.remove()
      history.addAll (future)
    }}

  def patch (update: Update): Unit =
    update match {
      case Assign (version, bytes, history) =>
        patch (version, bytes, history)
      case Patch (end, checksum, patches) =>
        patch (end, patches)
    }

  def patch (meta: Meta) (implicit disk: Disk): Async [Unit] =
    guard {
      for {
        (version, bytes, history) <- pager.read (meta.pos)
      } yield {
        patch (version, bytes, history)
      }}

  def checkpoint (meta: Meta): Unit =
    this.saved = Some (meta)

  def close() (implicit disk: Disk): Async [Handler] =
    guard {
      for {
        _ <- when (saved) (patch (_))
      } yield {
        new Handler (id, version, bytes.murmur32, bytes, history, saved)
      }}
}
