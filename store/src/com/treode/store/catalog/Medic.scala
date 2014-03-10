package com.treode.store.catalog

import java.util.ArrayDeque
import scala.collection.JavaConversions._

import com.treode.async.Async
import com.treode.disk.{Disks, Position}
import com.treode.store.{Bytes, CatalogId}

import Async.supply
import Poster.pager

private class Medic {

  var version = 0
  var bytes = Bytes.empty
  var history = new ArrayDeque [Bytes]

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

  def checkpoint (version: Int, bytes: Bytes, history: Seq [Bytes]): Unit =
    patch (version, bytes, history)

  def close (poster: Poster): Handler =
    new Handler (version, bytes.hashCode, bytes, history, poster)
}
