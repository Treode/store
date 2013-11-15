package com.treode.store.local

import com.treode.concurrent.Callback
import com.treode.store.{Bytes, TxClock}
import org.scalatest.Assertions

trait TestableTimedTable extends TimedTable with Assertions {

  def getAndExpect (key: Bytes, time: TxClock) (expected: TimedCell) {
    var actual: TimedCell = null
    get (key, time, new Callback [TimedCell] {
      def pass (_actual: TimedCell) = actual = _actual
      def fail (t: Throwable) = throw t
    })
    assert (actual != null, "Expected cell.")
    expectResult (expected) (actual)
  }

  def put (key: Bytes, time: TxClock, value: Option [Bytes]) {
    var mark = false
    put (key, time, value, new Callback [Unit] {
      def pass (v: Unit) = mark = true
      def fail (t: Throwable) = throw t
    })
    assert (mark, "Expected callback invokation.")
  }

  def toSeq: Seq [TimedCell]
}
