package com.treode.store.catalog

import java.util.ArrayDeque
import scala.collection.JavaConversions._

import com.treode.async.Async
import com.treode.disk.{Disks, Position}
import com.treode.store.{Bytes, CatalogId}

import Poster.pager

private class Medic (
  var version: Int,
  var bytes:  Bytes,
  var history: ArrayDeque [Bytes]
) {

  def patch (version: Int, bytes: Bytes, history: Seq [Bytes]) {
    if (this.version < version) {
      this.version = version
      this.bytes = bytes
      this.history.clear()
      this.history.addAll (history)
    }}

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

  def close (poster: Poster): Handler =
    new Handler (version, bytes.hashCode, bytes, history, poster)
}

private object Medic {

  def apply(): Medic =
    new Medic (0, Bytes.empty, new ArrayDeque)

  def apply (
      id: CatalogId,
      pos: Position
  ) (implicit
      reload: Disks.Reload
  ): Async [(CatalogId, Medic)] = {
    for {
      (version, bytes, _history) <- pager.read (reload, pos)
    } yield {
      val history = new ArrayDeque [Bytes]
      history.addAll (_history)
      (id, new Medic (version, bytes, history))
    }}}
