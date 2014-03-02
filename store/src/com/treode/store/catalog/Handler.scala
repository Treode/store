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
      Right (other, Seq.empty)
    } else if (start < 0) {
      Left (version, bytes, history.toSeq)
    } else {
      Right (other, history.drop (start) .toSeq)
    }}

  def patch (version: Int, bytes: Bytes, history: Seq [Bytes]) {
    if (this.version < version) {
      this.version = version
      this.bytes = bytes
      this.history.clear()
      this.history.addAll (history)
      poster.post (Left ((version, bytes, history)), bytes)
    }}

  def patch (start: Int, patches: Seq [Bytes]) {
    if (start + patches.length > version) {
      val now = version
      val future = patches drop (now - start)
      for (patch <- future) {
        version += 1
        bytes = Handler.patch (bytes, patch)
        history.add (patch)
        if (history.size > catalogHistoryLimit)
          history.remove()
      }
      poster.post (Right (now, future), bytes)
    }}

  def patch (update: Update): Unit =
    update match {
      case Left ((version, bytes, history)) =>
        patch (version, bytes, history)
      case Right ((start, patches)) =>
        patch (start, patches)
    }

  def issue (version: Int, bytes: Bytes) {
    require (
        version == this.version + 1,
        "Required version ${this.version + 1}, found $version.")
    val now = this.version
    val patch = Handler.diff (this.bytes, bytes)
    this.version = version
    this.bytes = bytes
    history.add (patch)
    if (history.size > catalogHistoryLimit)
      history.remove()
    poster.post (Right (now, Seq (patch)), bytes)
  }

  def checkpoint(): Async [(MailboxId, Position)] =
    guard {
      poster.checkpoint (version, bytes, materialize (history))
    }}

private object Handler {

  def diff (val0: Bytes, val1: Bytes): Bytes = {
    val differ = new Delta
    differ.setChunkSize (catalogChunkSize)
    Bytes (differ.compute (val0.bytes, val1.bytes))
  }

  def patch (val0: Bytes, diff: Bytes): Bytes = {
    val patcher = new GDiffPatcher
    Bytes (patcher.patch (val0.bytes, diff.bytes))
  }

  def apply (poster: Poster): Handler =
    new Handler (0, Bytes.empty, new ArrayDeque, poster)
}
