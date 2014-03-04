package com.treode.store.catalog

import java.util.ArrayDeque
import scala.collection.JavaConversions._

import com.nothome.delta.{Delta, GDiffPatcher}
import com.treode.async.{Async, Scheduler}
import com.treode.cluster.MailboxId
import com.treode.cluster.misc.materialize
import com.treode.disk.{Disks, Position}
import com.treode.store.Bytes

import Async.guard

private class Handler (
  var version: Int,
  var bytes:  Bytes,
  var history: ArrayDeque [Bytes],
  poster: Poster
) {

  def diff (other: Int): Update = {
    val start = other - version + history.size
    if (start >= history.size) {
      Patch (version, bytes.hashCode, Seq.empty)
    } else if (start < 0) {
      Assign (version, bytes, history.toSeq)
    } else {
      Patch (version, bytes.hashCode, history.drop (start) .toSeq)
    }}

  def patch (version: Int, bytes: Bytes, history: Seq [Bytes]) {
    if (this.version < version) {
      this.version = version
      this.bytes = bytes
      this.history.clear()
      this.history.addAll (history)
      poster.post (Assign (version, bytes, history), bytes)
    }}

  def patch (end: Int, patches: Seq [Bytes]) {
    val span = end - version
    if (0 < span && span <= patches.length) {
      val future = patches drop (patches.length - span)
      var bytes = this.bytes
      for (patch <- future)
        bytes = Patch.patch (bytes, patch)
      this.version += span
      this.bytes = bytes
      for (_ <- 0 until history.size + span - catalogHistoryLimit)
        history.remove()
      history.addAll (future)
      poster.post (Patch (version, bytes.hashCode, future), bytes)
    }}

  def patch (update: Update): Unit =
    update match {
      case Assign (version, bytes, history) =>
        patch (version, bytes, history)
      case Patch (end, checksum, patches) =>
        patch (end, patches)
    }

  def issue (version: Int, bytes: Bytes) {
    require (
        version == this.version + 1,
        "Required version ${this.version + 1}, found $version.")
    val patch = Patch.diff (this.bytes, bytes)
    this.version = version
    this.bytes = bytes
    history.add (patch)
    if (history.size > catalogHistoryLimit)
      history.remove()
    poster.post (Patch (version, bytes.hashCode, Seq (patch)), bytes)
  }

  def checkpoint(): Async [(MailboxId, Position)] =
    guard {
      poster.checkpoint (version, bytes, materialize (history))
    }}

private object Handler {

  def apply (poster: Poster): Handler =
    new Handler (0, Bytes.empty, new ArrayDeque, poster)
}
