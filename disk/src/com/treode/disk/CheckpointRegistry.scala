package com.treode.disk

import java.util.ArrayList

import com.treode.async.{Async, AsyncConversions}

import Async.guard
import AsyncConversions._

private class CheckpointRegistry {

  private val checkpoints = new ArrayList [Unit => Async [Unit]]

  def checkpoint (f: => Async [Unit]): Unit =
    checkpoints.add (_ => f )

  def checkpoint(): Async [Unit] =
    guard {
      checkpoints.latch.unit (_())
    }}
