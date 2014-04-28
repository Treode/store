package com.treode.disk

import java.util.ArrayList

import com.treode.async.Async
import com.treode.async.implicits._

import Async.guard

private class CheckpointRegistry {

  private val checkpoints = new ArrayList [Unit => Async [Unit]]

  def checkpoint (f: => Async [Unit]): Unit =
    checkpoints.add (_ => f )

  def checkpoint(): Async [Unit] =
    guard {
      checkpoints.latch.unit foreach (_())
    }}
