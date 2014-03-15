package com.treode.store.catalog

import java.util.ArrayDeque
import scala.collection.JavaConversions._

import com.treode.async.Async
import com.treode.disk.Disks
import com.treode.store.Bytes

import Async.{guard, when}
import Poster.{Meta, pager}

private class Medic {

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

  def patch (meta: Meta) (implicit disks: Disks): Async [Unit] =
    guard {
      for {
        (version, bytes, history) <- pager.read (meta.pos)
      } yield {
        patch (version, bytes, history)
      }}

  def checkpoint (meta: Meta): Unit =
    this.saved = Some (meta)

  def close (poster: Poster) (implicit disks: Disks): Async [Handler] =
    guard {
      for {
        _ <- when (saved.isDefined) (patch (saved.get))
      } yield {
        new Handler (version, bytes.hashCode, bytes, history, saved, poster)
      }}
}
