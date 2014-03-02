package com.treode.store.catalog

import java.util.ArrayDeque
import scala.collection.JavaConversions._

import com.treode.async.Async
import com.treode.cluster.MailboxId
import com.treode.disk.{Disks, Position}
import com.treode.store.Bytes

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
      }}}

  def patch (update: Update): Unit =
    update match {
      case Left ((version, bytes, history)) =>
        patch (version, bytes, history)
      case Right ((start, patches)) =>
        patch (start, patches)
    }

  def close (poster: Poster): Handler =
    new Handler (version, bytes, history, poster)
}

private object Medic {

  def apply(): Medic =
    new Medic (0, Bytes.empty, new ArrayDeque)

  def apply (
      id: MailboxId,
      pos: Position
  ) (implicit
      reload: Disks.Reload
  ): Async [(MailboxId, Medic)] = {
    for {
      (version, bytes, _history) <- pager.read (reload, pos)
    } yield {
      val history = new ArrayDeque [Bytes]
      history.addAll (_history)
      (id, new Medic (version, bytes, history))
    }}}
