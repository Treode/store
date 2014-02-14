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
      _checkpoint (disks.panic)
    else
      engaged = false
  }

  private val completed: Callback [Unit] =
    new Callback [Unit] {

      def pass (v: Unit) {
        fiber.execute (reengage())
      }

      def fail (t: Throwable) {
        disks.panic (t)
      }}

  private def _checkpoint (cb: Callback [Unit]) {

    bytes = 0
    entries = 0
    checkreq = false
    engaged = true

    val superblockWritten = fiber.callback (cb) { _: Unit =>
      rootgen += 1
      reengage()
    }

    val rootsWritten = fiber.continue (cb) { pos: Position =>
      disks.checkpoint (rootgen+1, pos, superblockWritten)
    }

    val logsMarked = fiber.continue (cb) { _: Unit =>
      checkpoints.checkpoint (rootgen+1, rootsWritten)
    }

    disks.mark (logsMarked)
  }

  def launch (checkpoints: CheckpointRegistry): Unit =
    fiber.execute {
      this.checkpoints = checkpoints
      reengage()
    }

  def checkpoint(): Unit =
    fiber.execute {
      if (!engaged)
        _checkpoint (disks.panic)
      else
        checkreq = true
    }

  def tally (bytes: Int, entries: Int): Unit =
    fiber.execute {
      this.bytes += bytes
      this.entries += entries
      if (!engaged && config.checkpoint (this.bytes, this.entries))
        _checkpoint (disks.panic)
    }}
