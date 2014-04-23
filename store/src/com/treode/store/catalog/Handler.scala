package com.treode.store.catalog

import java.util.ArrayDeque
import scala.collection.JavaConversions._

import com.nothome.delta.{Delta, GDiffPatcher}
import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.misc.materialize
import com.treode.disk.{Disks, PageDescriptor, Position, RecordDescriptor}
import com.treode.store.{Bytes, CatalogId, StaleException}

import Async.{guard, when}
import Callback.ignore
import Handler.{Meta, pager}

private class Handler (
    val id: CatalogId,
    var version: Int,
    var checksum: Int,
    var bytes:  Bytes,
    var history: ArrayDeque [Bytes],
    var saved: Option [Meta]
) (implicit
    disks: Disks
) {

  def diff (other: Int): Update = {
    val start = other - version + history.size
    if (start >= history.size) {
      Patch (version, checksum, Seq.empty)
    } else if (start < 0) {
      Assign (version, bytes, history.toSeq)
    } else {
      Patch (version, checksum, history.drop (start) .toSeq)
    }}

  def patch (version: Int, bytes: Bytes, history: Seq [Bytes]): Boolean = {
    if (version <= this.version)
      return false
    this.version = version
    this.checksum = bytes.hashCode
    this.bytes = bytes
    this.history.clear()
    this.history.addAll (history)
    Handler.update.record ((id, Assign (version, bytes, history))) run (ignore)
    true
  }

  def patch (end: Int, patches: Seq [Bytes]) : Boolean ={
    val span = end - version
    if (span <= 0 || patches.length < span)
      return false
    val future = patches drop (patches.length - span)
    var bytes = this.bytes
    for (patch <- future)
      bytes = Patch.patch (bytes, patch)
    this.version += span
    this.checksum = bytes.hashCode
    this.bytes = bytes
    for (_ <- 0 until history.size + span - catalogHistoryLimit)
      history.remove()
    history.addAll (future)
    Handler.update.record ((id, Patch (version, checksum, future))) run (ignore)
    true
  }

  def patch (update: Update): Boolean =
    update match {
      case Assign (version, bytes, history) =>
        patch (version, bytes, history)
      case Patch (end, checksum, patches) =>
        patch (end, patches)
    }

  def diff (version: Int, bytes: Bytes): Patch = {
    if (version != this.version + 1)
      throw new StaleException
    Patch (version, checksum, Seq (Patch.diff (this.bytes, bytes)))
  }

  def probe (groups: Set [Int]): Set [Int] =
    if (saved.isDefined)
      Set (saved.get.version)
    else
      Set.empty

  def save(): Async [Unit] =
    guard {
      val history = materialize (this.history)
      for {
        pos <- pager.write (id.id, version, (version, bytes, history))
        meta = Meta (version, pos)
        _ <- Handler.checkpoint.record (id, meta)
      } yield {
        this.saved = Some (meta)
      }}

  def compact (groups: Set [Int]): Async [Unit] =
    when (saved.isDefined && (groups contains saved.get.version)) (save())

  def checkpoint(): Async [Unit] =
    guard {
      saved match {
        case Some (meta) if meta.version == version =>
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

  def apply (id: CatalogId) (implicit disks: Disks): Handler =
    new Handler (id, 0, 0, Bytes.empty, new ArrayDeque, None)
}
