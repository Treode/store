package com.treode.disk

import java.util.ArrayList
import scala.collection.JavaConversions._

import com.treode.async.{Async, AsyncConversions}

import Async.guard
import AsyncConversions._

private class CheckpointRegistry (implicit disks: DiskDrives) {
  import disks.{config}

  private val checkpoints = new ArrayList [Unit => Async [Unit]]

  def checkpoint (f: => Async [Unit]): Unit =
    checkpoints.add (_ => f )

  def checkpoint(): Async [Unit] =
    guard {
      checkpoints.latch.unit (_())
    }}
