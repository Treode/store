package com.treode.disk

import scala.collection.immutable.Queue
import com.treode.async.{Async, Callback, Fiber}

import Callback.ignore

private class Checkpointer (kit: DisksKit) {
  import kit.{config, disks, scheduler}

  val fiber = new Fiber
  var checkpoints: CheckpointRegistry = null
  var bytes = 0
  var entries = 0
  var checkreqs = Queue.empty [Callback [Unit]]
  var engaged = true

  private def reengage() {
    if (!checkreqs.isEmpty) {
      val (first, rest) = checkreqs.dequeue
      checkreqs = rest
      _checkpoint (first)
    } else if (config.checkpoint (bytes, entries)) {
      _checkpoint (ignore)
    } else {
      engaged = false
    }}

  private def _checkpoint (cb: Callback [Unit]) {
    engaged = true
    bytes = 0
    entries = 0
    val task = for {
      marks <- disks.mark()
      _ <- checkpoints.checkpoint()
      _ <- disks.checkpoint (marks)
    } yield fiber.execute {
      reengage()
    }
    task run (cb)
  }

  def launch (checkpoints: CheckpointRegistry): Async [Unit] =
    fiber.supply {
      this.checkpoints = checkpoints
      reengage()
    }

  def checkpoint(): Async [Unit] =
    fiber.async { cb =>
      if (engaged)
        checkreqs = checkreqs.enqueue (cb)
      else
        _checkpoint (cb)
    }

  def tally (bytes: Int, entries: Int): Unit =
    fiber.execute {
      this.bytes += bytes
      this.entries += entries
      if (!engaged && checkreqs.isEmpty && config.checkpoint (this.bytes, this.entries))
        _checkpoint (ignore)
    }}
