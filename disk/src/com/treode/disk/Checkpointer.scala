package com.treode.disk

import com.treode.async.{Callback, Fiber}

private class Checkpointer (disks: DiskDrives) {
  import disks.{config, scheduler}

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

  private val completed: Callback [Unit] =
    new Callback [Unit] {
      def pass (v: Unit): Unit = fiber.execute (reengage())
      def fail (t: Throwable): Unit = disks.panic (t)
    }

  private def _checkpoint() {
    bytes = 0
    entries = 0
    checkreq = false
    engaged = true
    val task = for {
      _ <- disks.mark()
      pos <- fiber.flatten (checkpoints.checkpoint (rootgen+1))
      _ <- fiber.flatten (disks.checkpoint (rootgen+1, pos))
      _ <- fiber.supply {
          rootgen+=1;
          reengage()
      }
    } yield ()
    task run completed
  }

  def launch (checkpoints: CheckpointRegistry): Unit =
    fiber.execute {
      this.checkpoints = checkpoints
      reengage()
    }

  def checkpoint(): Unit =
    fiber.execute {
      if (!engaged)
        _checkpoint()
      else
        checkreq = true
    }

  def tally (bytes: Int, entries: Int): Unit =
    fiber.execute {
      this.bytes += bytes
      this.entries += entries
      if (!engaged && config.checkpoint (this.bytes, this.entries))
        _checkpoint()
    }}
