package com.treode.disk

import scala.util.{Failure, Success}
import com.treode.async.{Async, Callback, Fiber}

private class Checkpointer (kit: DisksKit) {
  import kit.{config, disks, panic, scheduler}

  val fiber = new Fiber (scheduler)
  var checkpoints: CheckpointRegistry = null
  var rootgen = 0
  var bytes = 0
  var entries = 0
  var checkreq = false
  var engaged = true

  private def reengage() {
    if (checkreq || config.checkpoint (bytes, entries))
      _checkpoint()
    else
      engaged = false
  }

  private val completed: Callback [Unit] = {
    case Success (v) => fiber.execute (reengage())
    case Failure (t) => panic (t)
  }

  private def _checkpoint(): Unit =
    fiber.run (completed) {
      bytes = 0
      entries = 0
      checkreq = false
      engaged = true
      for {
        _ <- disks.mark()
        _ <- fiber.guard (checkpoints.checkpoint())
        _ <- fiber.guard (disks.checkpoint())
        _ <- fiber.supply {
            rootgen += 1
            reengage()
        }
      } yield ()
    }

  def launch (checkpoints: CheckpointRegistry): Async [Unit] =
    fiber.supply {
      this.checkpoints = checkpoints
      reengage()
    }

  def checkpoint(): Unit = {
    fiber.execute {
      if (!engaged)
        _checkpoint()
      else
        checkreq = true
    }}

  def tally (bytes: Int, entries: Int): Unit =
    fiber.execute {
      this.bytes += bytes
      this.entries += entries
      if (!engaged && config.checkpoint (this.bytes, this.entries))
        _checkpoint()
    }}
