package com.treode.store.local

import java.util.ArrayList
import scala.collection.JavaConversions._
import scala.language.postfixOps

import com.treode.concurrent.Callback
import com.treode.store.{MultiException, WriteOp}

private class TimedCommitter (ops: Seq [WriteOp], cb: Callback [Unit]) extends Callback [Unit] {

  private var _awaiting = ops.size
  private var _failures = new ArrayList [Throwable]

  private def finish() {
    if (!_failures.isEmpty) {
      cb.fail (MultiException (_failures.toSeq))
    } else {
      cb.apply()
    }}

  def pass (v: Unit) {
    val ready = synchronized {
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
