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

import com.treode.buffer.PagedBuffer
import com.treode.async.{Async, Callback}
import com.treode.async.io.File

import Async.guard
import PageLedger.{Groups, Projector, Zipped, longBytes, varIntBytes, varLongBytes}

private class PageLedger (
    private var ledger: Map [(TypeId, ObjectId, GroupId), Long],
    private var objects: Set [(TypeId, ObjectId)],
    private var _byteSize: Int
) extends Traversable [(TypeId, ObjectId, GroupId, Long)] {

  def this() =
    this (Map.empty, Set.empty, varLongBytes + varIntBytes)

  def foreach [U] (f: ((TypeId, ObjectId, GroupId, Long)) => U) {
    for (((typ, obj, group), totalBytes) <- ledger)
      f (typ, obj, group, totalBytes)
  }

  def add (typ: TypeId, obj: ObjectId, group: GroupId, pageBytes: Long) {

    val okey = (typ, obj)

    if (!(objects contains okey)) {
      objects += okey
      _byteSize += 2*longBytes // typeId, objectId
    }

    val key = ((typ, obj, group))
    ledger get (key) match {
      case Some (totalBytes) =>
        ledger += key -> (totalBytes + pageBytes)
      case None =>
        ledger += key -> pageBytes
        _byteSize += longBytes + longBytes // group, page bytes
    }}

  def add (other: Traversable [(TypeId, ObjectId, GroupId, Long)]) {
    for ((typ, obj, group, totalBytes) <- other)
      add (typ, obj, group, totalBytes)
  }

  def byteSize = _byteSize

  def get (typ: TypeId, obj: ObjectId, group: GroupId): Long =
    ledger.get ((typ, obj, group)) .getOrElse (0)

  def groups: Map [(TypeId, ObjectId), Set [GroupId]] =
    ledger.keys.groupBy (e => (e._1, e._2)) .mapValues (_.map (_._3) .toSet)

  def liveBytes (liveGroups: Groups): Long = {
    var liveBytes = 0L
    for {
      ((typ, obj), pageGroups) <- liveGroups
      group <- pageGroups
    } liveBytes += get (typ, obj, group)
    liveBytes
  }

  def project: Projector =
    new Projector (objects, ledger.keySet, _byteSize)

  def zip: Zipped = {
    var builder = Map.empty [(TypeId, ObjectId), Seq [(GroupId, Long)]]
    for (((id, obj, group), totalBytes) <- ledger) {
      builder get (id, obj) match {
        case Some (groups) =>
          builder += (id, obj) -> ((group, totalBytes) +: groups)
        case None =>
          builder += (id, obj) -> Seq ((group, totalBytes))
      }}
    new Zipped (builder.map {case ((id, obj), groups) => (id, obj, groups)} .toSeq)
  }

  override def isEmpty = ledger.isEmpty

  override def clone(): PageLedger =
    new PageLedger (ledger, objects, _byteSize)
}

private object PageLedger {

  val varIntBytes = 5
  val varLongBytes = 9
  val longBytes = 8

  type Groups = Map [(TypeId, ObjectId), Set [GroupId]]

  class Merger {

    private var _groups = Map.empty [(TypeId, ObjectId), Set [GroupId]]

    def add (groups: Groups) {
      for (((typ, obj), gs1) <- groups)
        _groups.get (typ, obj) match {
          case Some (gs0) => _groups += ((typ, obj) -> (gs0 ++ gs1))
          case None       => _groups += ((typ, obj) -> gs1)
      }}

    def result: Groups =
      _groups
  }

  class Projector (
      private var objects: Set [(TypeId, ObjectId)],
      private var groups: Set [(TypeId, ObjectId, GroupId)],
      private var _byteSize: Int) {

    def this() =
      this (Set.empty, Set.empty, varIntBytes)

    def add (typ: TypeId, obj: ObjectId, group: GroupId) {
      val okey = (typ, obj)
      val gkey = (typ, obj, group)
      if (!(objects contains okey)) {
        objects += okey
        _byteSize += 2*longBytes // typeId, objectId
      }
      if (!(groups contains gkey)) {
        groups += gkey
        _byteSize += longBytes + longBytes // group, page bytes
      }}

    def byteSize = _byteSize
  }

  class Zipped (private val ledger: Seq [(TypeId, ObjectId, Seq [(GroupId, Long)])])
  extends Traversable [(TypeId, ObjectId, GroupId, Long)] {

    def foreach [U] (f: ((TypeId, ObjectId, GroupId, Long)) => U) {
      for {
        (typ, obj, groups) <- ledger
        (group, totalBytes) <- groups
      } f (typ, obj, group, totalBytes)
    }

    def unzip: PageLedger = {
      var ledger = Map.empty [(TypeId, ObjectId, GroupId), Long]
      var objects = Set.empty [(TypeId, ObjectId)]
      var byteSize = varIntBytes // entry count
      for ((typ, obj, group, totalBytes) <- this) {
        ledger += (typ, obj, group) -> totalBytes
        objects += ((typ, obj))
        byteSize += 4 * longBytes // typeId, objectId, group, page bytes
      }
      new PageLedger (ledger, objects, byteSize)
    }}

  object Zipped {

    val empty = new Zipped (Seq.empty)

    val pickler = {
      import DiskPicklers._
      // Tagged for forwards compatibility.
      tagged [Zipped] (
          0x00863FA19918F4DAL ->
              wrap (seq (tuple (typeId, objectId, seq (tuple (groupId, ulong)))))
                  .build (new Zipped (_))
                  .inspect (_.ledger))
    }}

  def merge (groups: Seq [Groups]): Groups = {
    val merger = new Merger
    groups foreach (merger.add _)
    merger.result
  }

  def read (file: File, geom: DriveGeometry, pos: Long): Async [PageLedger] =
    guard {
      val buf = PagedBuffer (math.max (12, geom.blockBits))
      for (_ <- file.deframe (checksum, buf, pos, geom.blockBits))
        yield Zipped.pickler.unpickle (buf) .unzip
    }

  def write (ledger: Zipped, file: File, geom: DriveGeometry, pos: Long, limit: Long): Async [Unit] =
    guard {
      val buf = PagedBuffer (geom.blockBits)
      Zipped.pickler.frame (checksum, ledger, buf)
      buf.writePos = geom.blockAlignUp (buf.writePos)
      if (buf.writePos > limit - pos)
        throw new PageLedgerOverflowException
      file.flush (buf, pos)
    }

  def write (ledger: PageLedger, file: File, geom: DriveGeometry, pos: Long, limit: Long): Async [Unit] =
    guard {
      write (ledger.zip, file, geom, pos, limit)
    }}
