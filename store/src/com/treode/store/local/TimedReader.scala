package com.treode.store.local

import java.util.ArrayList
import scala.collection.JavaConversions.asScalaBuffer

import com.treode.store.{MultiException, ReadBatch, ReadCallback, TimedCell, Value}

private class TimedReader (batch: ReadBatch, cb: ReadCallback) {

  private var _awaiting = batch.ops.length
  private val _got = new Array [Value] (batch.ops.length)
  private val _failures = new ArrayList [Throwable]

  private def finish() {
    if (!_failures.isEmpty)
      cb.fail (MultiException (_failures.toSeq))
    else
      cb.apply (_got.toSeq)
  }

  def rt = batch.rt

  def got (n: Int, c: TimedCell) {
    val ready = synchronized {
      _got (n) = Value (c.time, c.value)
      _awaiting -= 1
      _awaiting == 0
    }
    if (ready)
      finish()
  }

  def fail (t: Throwable) {
    val ready = synchronized {
      _failures.add (t)
      _awaiting -= 1
      _awaiting == 0
    }
    if (ready)
      finish()
  }}
