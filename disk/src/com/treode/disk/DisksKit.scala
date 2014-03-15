package com.treode.disk

import scala.util.{Failure, Success}

import com.treode.async.{Async, Callback, Scheduler}

import Async.{guard, latch}

private class DisksKit (implicit val scheduler: Scheduler, val config: DisksConfig) {

  val logd = new Dispatcher [PickledRecord] (scheduler)
  val paged = new Dispatcher [PickledPage] (scheduler)
  val disks = new DiskDrives (this)
  val checkpointer = new Checkpointer (this)
  val releaser = new Releaser
  val compactor = new Compactor (this)

  def launch (checkpoints: CheckpointRegistry, pages: PageRegistry): Unit =
    guard {
      for {
        _ <- latch (
            checkpointer.launch (checkpoints),
            compactor.launch (pages))
        _ <- disks.launch()
      } yield ()
    } run (panic)

  def panic (t: Throwable) {
    throw t
  }

  val panic: Callback [Unit] = {
    case Success (v) => ()
    case Failure (t) => panic (t)
  }}
