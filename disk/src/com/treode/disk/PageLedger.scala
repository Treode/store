package com.treode.disk

import com.treode.buffer.PagedBuffer
import com.treode.async.{Callback, callback, guard}
import com.treode.async.io.File

import PageLedger.{Zipped, intBytes, longBytes}

class PageLedger (
    private var ledger: Map [(TypeId, PageGroup), Long],
    private var ids: Set [TypeId],
    private var _byteSize: Int
) extends Traversable [(TypeId, PageGroup, Long)] {

  def this() =
    this (Map.empty, Set.empty, 0)

  def foreach [U] (f: ((TypeId, PageGroup, Long)) => U) {
    for (((id, group), totalBytes) <- ledger)
      f (id, group, totalBytes)
  }

  def add (id: TypeId, group: PageGroup, pageBytes: Long) {

    if (!(ids contains id)) {
      ids += id
      _byteSize += intBytes // typeId
    }

    val key = ((id, group))
    ledger get (key) match {
      case Some (totalBytes) =>
        ledger += key -> (totalBytes + pageBytes)
      case None =>
        ledger += key -> pageBytes
        _byteSize += group.byteSize + longBytes // group, page bytes
    }}

  def add (other: Traversable [(TypeId, PageGroup, Long)]) {
    for ((id, group, totalBytes) <- other)
      add (id, group, totalBytes)
  }

  def byteSize = _byteSize

  def get (id: TypeId, group: PageGroup): Long =
    ledger.get ((id, group)) .getOrElse (0)

  def groups: Map [TypeId, Set [PageGroup]] =
    ledger.keys.groupBy (_._1) .mapValues (_.map (_._2) .toSet)

  def zip: Zipped = {
    var builder = Map.empty [TypeId, Seq [(PageGroup, Long)]]
    for (((id, group), totalBytes) <- ledger) {
      builder get (id) match {
        case Some (groups) =>
          builder += id -> ((group, totalBytes) +: groups)
        case None =>
          builder += id -> Seq ((group, totalBytes))
      }}
    new Zipped (builder.toSeq)
  }

  override def clone(): PageLedger =
    new PageLedger (ledger, ids, byteSize)
}

object PageLedger {

  val intBytes = 5
  val longBytes = 9

  class Zipped (private val ledger: Seq [(TypeId, Seq [(PageGroup, Long)])])
  extends Traversable [(TypeId, PageGroup, Long)] {

    def foreach [U] (f: ((TypeId, PageGroup, Long)) => U) {
      for {
        (id, groups) <- ledger
        (group, totalBytes) <- groups
      } f (id, group, totalBytes)
    }

    def unzip: PageLedger = {
      var ledger = Map.empty [(TypeId, PageGroup), Long]
      var ids = Set.empty [TypeId]
      var byteSize = intBytes // entry count
      for ((id, group, totalBytes) <- this) {
        ledger += (id, group) -> totalBytes
        ids += id
        byteSize += intBytes + group.byteSize + longBytes // typeid, group, page bytes
      }
      new PageLedger (ledger, ids, byteSize)
    }}

  object Zipped {

    val empty = new Zipped (Seq.empty)

    val pickler = {
      import DiskPicklers._
      wrap (seq (tuple (typeId, seq (tuple (pageGroup, long)))))
      .build (new Zipped (_))
      .inspect (_.ledger)
    }}

  def read (file: File, pos: Long, cb: Callback [PageLedger]): Unit =
    guard (cb) {
      val buf = PagedBuffer (12)
      file.deframe (buf, pos, callback (cb) { _ =>
        Zipped.pickler.unpickle (buf) .unzip
      })
    }

  def write (ledger: PageLedger, file: File, pos: Long, cb: Callback [Unit]): Unit =
    guard (cb) {
      val buf = PagedBuffer (12)
      Zipped.pickler.frame (ledger.zip, buf)
      file.flush (buf, pos, cb)
    }

  def write (ledger: Zipped, file: File, pos: Long, cb: Callback [Unit]): Unit =
    guard (cb) {
      val buf = PagedBuffer (12)
      Zipped.pickler.frame (ledger, buf)
      file.flush (buf, pos, cb)
    }}
