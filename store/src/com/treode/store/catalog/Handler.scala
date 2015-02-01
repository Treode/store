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

import com.nothome.delta.{Delta, GDiffPatcher}
import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.misc.materialize
import com.treode.disk.{Disk, PageDescriptor, Position, RecordDescriptor}
import com.treode.store.{Bytes, CatalogId}

import Async.{guard, when}
import Callback.ignore
import Handler.{Meta, pager}

private class Handler (
    val id: CatalogId,
    val chronicle: Chronicle,
    var saved: Option [Meta]
) (implicit
    disk: Disk
) {

  def version = chronicle.version
  def checksum = chronicle.checksum
  def bytes = chronicle.bytes
  def history = chronicle.history

  def this(id: CatalogId,
    version: Int,
    checksum: Int,
    bytes: Bytes,
    history: ArrayDeque [Bytes],
    saved: Option [Meta]
  ) (implicit disk:Disk) {
    this (id, new Chronicle (version, checksum, bytes, history), saved)
  }

  def diff (other: Int): Update = {
    chronicle.diff (other)
  }

  def diff (version: Int, bytes: Bytes): Patch = {
    chronicle.diff (version, bytes)
  }

  /*
   * Update your own Chronicle to the given (version, bytes, history)
   */
  def patch (version: Int, bytes: Bytes, history: Seq [Bytes]): Boolean = {
    chronicle.patch (version, bytes, history) match {
      case Some (update) =>
        Handler.update.record ((id, update)) run (ignore)
        true
      case None =>
        false
    }
  }

  def patch (end: Int, checksum: Int, patches: Seq [Bytes]) : Boolean = {
    chronicle.patch (end, checksum, patches) match {
      case Some (update) =>
        Handler.update.record ((id, update)) run (ignore)
        true
      case None =>
        false
    }
  }

  def patch (update: Update): Boolean =
    update match {
      case Assign (version, bytes, history) =>
        patch (version, bytes, history)
      case Patch (end, checksum, patches) =>
        patch (end, checksum, patches)
    }

  def probe (groups: Set [Int]): Set [Int] =
    if (saved.isDefined)
      Set (saved.get.version)
    else
      Set.empty

  def save(): Async [Unit] =
    guard {
      val history = materialize (chronicle.history)
      for {
        pos <- pager.write (id.id, chronicle.version, (version, bytes, history))
        meta = Meta (chronicle.version, pos)
        _ <- Handler.checkpoint.record (id, meta)
      } yield {
        this.saved = Some (meta)
      }}

  def compact (groups: Set [Int]): Async [Unit] =
    when (saved.isDefined && (groups contains saved.get.version)) (save())

  def checkpoint(): Async [Unit] =
    guard {
      saved match {
        case Some (meta) if meta.version == chronicle.version =>
          Handler.checkpoint.record (id, saved.get)
        case _ =>
          save()
      }}}

private object Handler {

  case class Meta (version: Int, pos: Position)

  object Meta {

    val pickler = {
      import CatalogPicklers._
      wrap (uint, pos)
      .build ((Meta.apply _).tupled)
      .inspect (v => (v.version, v.pos))
    }}

  case class Post (update: Update, bytes: Bytes)

  val update = {
    import CatalogPicklers._
    RecordDescriptor (0x7F5551148920906EL, tuple (catId, Update.pickler))
  }

  val checkpoint = {
    import CatalogPicklers._
    RecordDescriptor (0x79C3FDABEE2C9FDFL, tuple (catId, Meta.pickler))
  }

  val pager = {
    import CatalogPicklers._
    PageDescriptor (0x8407E7035A50C6CFL, uint, tuple (uint, bytes, seq (bytes)))
  }

  def apply (id: CatalogId) (implicit disk: Disk): Handler =
    new Handler (id, 0, Bytes.empty.murmur32, Bytes.empty, new ArrayDeque, None)
}
