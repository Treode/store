package com.treode.store.local

import java.util.ArrayList
import scala.collection.JavaConversions.asScalaBuffer

import com.treode.async.MultiException
import com.treode.store.{ReadCallback, ReadOp, TimedCell, TxClock, Value}

private class TimedReader (val rt: TxClock, ops: Seq [ReadOp], cb: ReadCallback) {

  private var _awaiting = ops.length
  private val _got = new Array [Value] (ops.length)
  private val _failures = new ArrayList [Throwable]

  private def finish() {
    if (!_failures.isEmpty)
      cb.fail (MultiException.fit (_failures.toSeq))
    else
      cb.apply (_got.toSeq)
  }

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
