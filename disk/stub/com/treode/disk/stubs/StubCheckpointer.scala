package com.treode.disk.stubs

import scala.collection.immutable.Queue
import scala.util.Random

import com.treode.async.{Async, Callback, Fiber, Scheduler}
import com.treode.disk.CheckpointRegistry

import Callback.ignore

private class StubCheckpointer (implicit
    random: Random,
    scheduler: Scheduler,
    disk: StubDiskDrive,
    config: StubConfig
) {

  import config.checkpointProbability

  val fiber = new Fiber
  var checkpoints: CheckpointRegistry = null
  var checkreqs = Queue.empty [Callback [Unit]]
  var engaged = true

  private def reengage() {
    if (!checkreqs.isEmpty) {
      val (first, rest) = checkreqs.dequeue
      checkreqs = rest
      _checkpoint (first)
    } else {
      engaged = false
    }}

  private def _checkpoint (cb: Callback [Unit]) {
    engaged = true
    val mark = disk.mark()
    checkpoints .checkpoint() .map { _ =>
      disk.checkpoint (mark)
      fiber.execute (reengage())
    } .run (cb)
  }

  def launch (checkpoints: CheckpointRegistry): Unit =
    fiber.execute {
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

  def tally(): Unit =
    fiber.execute {
      if (checkpointProbability > 0.0 && random.nextDouble < checkpointProbability)
        if (checkreqs.isEmpty)
          if (engaged)
            checkreqs = checkreqs.enqueue (ignore [Unit])
          else
            _checkpoint (ignore)
    }}
