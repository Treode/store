package com.treode.store.local

import com.treode.concurrent.CallbackCaptor
import com.treode.store.{Bytes, TxClock}
import org.scalatest.Assertions

private trait TestableTimedTable extends TimedTable with Assertions {

  def getAndExpect (key: Bytes, time: TxClock) (expected: TimedCell) {
    val cb = new CallbackCaptor [TimedCell]
    get (key, time, cb)
    expectResult (expected) (cb.passed)
  }

  def put (key: Bytes, time: TxClock, value: Option [Bytes]) {
    val cb = new CallbackCaptor [Unit]
    put (key, time, value, cb)
    cb.passed
  }

  def toSeq: Seq [TimedCell]
}
