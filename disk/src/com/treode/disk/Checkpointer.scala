package com.treode.disk

import scala.util.{Failure, Success}
import com.treode.async.{Async, Callback, Fiber}

import Callback.ignore

private class Checkpointer (kit: DisksKit) {
  import kit.{config, disks, scheduler}

  val fiber = new Fiber (scheduler)
  var checkpoints: CheckpointRegistry = null
  var rootgen = 0
  var bytes = 0
  var entries = 0
  var checkreq = false
  var engaged = true

  private def reengage() {
    engaged = false
    if (checkreq || config.checkpoint (bytes, entries))
      _checkpoint()
  }

  private def _checkpoint() {
    if (engaged) {
      checkreq = true
      return
    }
    checkreq = false
    engaged = true
    fiber.guard {
      bytes = 0
      entries = 0
      for {
        _ <- disks.mark()
        _ <- checkpoints.checkpoint()
        _ <- disks.checkpoint()
      } yield fiber.execute {
        rootgen += 1
        reengage()
      }
    } run (ignore)
  }

  def launch (checkpoints: CheckpointRegistry): Async [Unit] =
    fiber.supply {
      this.checkpoints = checkpoints
      reengage()
    }

  def checkpoint(): Unit = {
    fiber.execute {
      _checkpoint()
    }}

  def tally (bytes: Int, entries: Int): Unit =
    fiber.execute {
      this.bytes += bytes
      this.entries += entries
      if (config.checkpoint (this.bytes, this.entries))
        _checkpoint()
    }}
