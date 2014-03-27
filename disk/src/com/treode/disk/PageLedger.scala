package com.treode.disk

import com.treode.buffer.PagedBuffer
import com.treode.async.{Async, Callback}
import com.treode.async.io.File

import Async.guard
import PageLedger.{Groups, Projector, Zipped, longBytes, varIntBytes}

class PageLedger (
    private var ledger: Map [(TypeId, ObjectId, PageGroup), Long],
    private var objects: Set [(TypeId, ObjectId)],
    private var _byteSize: Int
) extends Traversable [(TypeId, ObjectId, PageGroup, Long)] {

  def this() =
    this (Map.empty, Set.empty, varIntBytes)

  def foreach [U] (f: ((TypeId, ObjectId, PageGroup, Long)) => U) {
    for (((typ, obj, group), totalBytes) <- ledger)
      f (typ, obj, group, totalBytes)
  }

  def add (typ: TypeId, obj: ObjectId, group: PageGroup, pageBytes: Long) {

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
        _byteSize += group.byteSize + longBytes // group, page bytes
    }}

  def add (other: Traversable [(TypeId, ObjectId, PageGroup, Long)]) {
    for ((typ, obj, group, totalBytes) <- other)
      add (typ, obj, group, totalBytes)
  }

  def byteSize = _byteSize

  def get (typ: TypeId, obj: ObjectId, group: PageGroup): Long =
    ledger.get ((typ, obj, group)) .getOrElse (0)

  def groups: Map [(TypeId, ObjectId), Set [PageGroup]] =
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
    var builder = Map.empty [(TypeId, ObjectId), Seq [(PageGroup, Long)]]
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

object PageLedger {

  val varIntBytes = 5
  val longBytes = 8

  type Groups = Map [(TypeId, ObjectId), Set [PageGroup]]

  class Merger {

    private var _groups = Map.empty [(TypeId, ObjectId), Set [PageGroup]]

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
      private var groups: Set [(TypeId, ObjectId, PageGroup)],
      private var _byteSize: Int) {

    def this() =
      this (Set.empty, Set.empty, varIntBytes)

    def add (typ: TypeId, obj: ObjectId, group: PageGroup) {
      val okey = (typ, obj)
      val gkey = (typ, obj, group)
      if (!(objects contains okey)) {
        objects += okey
        _byteSize += 2*longBytes // typeId, objectId
      }
      if (!(groups contains gkey)) {
        groups += gkey
        _byteSize += group.byteSize + longBytes // group, page bytes
      }}

    def byteSize = _byteSize
  }

  class Zipped (private val ledger: Seq [(TypeId, ObjectId, Seq [(PageGroup, Long)])])
  extends Traversable [(TypeId, ObjectId, PageGroup, Long)] {

    def foreach [U] (f: ((TypeId, ObjectId, PageGroup, Long)) => U) {
      for {
        (typ, obj, groups) <- ledger
        (group, totalBytes) <- groups
      } f (typ, obj, group, totalBytes)
    }

    def unzip: PageLedger = {
      var ledger = Map.empty [(TypeId, ObjectId, PageGroup), Long]
      var objects = Set.empty [(TypeId, ObjectId)]
      var byteSize = varIntBytes // entry count
      for ((typ, obj, group, totalBytes) <- this) {
        ledger += (typ, obj, group) -> totalBytes
        objects += ((typ, obj))
        byteSize += 3*longBytes + group.byteSize // typeId, objectId, group, page bytes
      }
      new PageLedger (ledger, objects, byteSize)
    }}

  object Zipped {

    val empty = new Zipped (Seq.empty)

    val pickler = {
      import DiskPicklers._
      wrap (seq (tuple (typeId, objectId, seq (tuple (pageGroup, ulong)))))
      .build (new Zipped (_))
      .inspect (_.ledger)
    }}

  def merge (groups: Seq [Groups]): Groups = {
    val merger = new Merger
    groups foreach (merger.add _)
    merger.result
  }

  def read (file: File, pos: Long): Async [PageLedger] =
    guard {
      val buf = PagedBuffer (12)
      for (_ <- file.deframe (checksum, buf, pos))
        yield Zipped.pickler.unpickle (buf) .unzip
    }

  def write (ledger: Zipped, file: File, pos: Long): Async [Unit] =
    guard {
      val buf = PagedBuffer (12)
      Zipped.pickler.frame (checksum, ledger, buf)
      file.flush (buf, pos)
    }

  def write (ledger: PageLedger, file: File, pos: Long): Async [Unit] =
    guard {
      write (ledger.zip, file, pos)
    }}
