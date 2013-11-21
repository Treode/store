package com.treode.store.local

import scala.util.Random

import com.treode.async.{Callback, CallbackCaptor}
import com.treode.store._
import org.scalatest.Assertions

import Assertions._

private object LocalTimedTestTools extends TimedTestTools {

  implicit class RichCellIterator (iter: TimedIterator) {

    def toSeq: Seq [TimedCell] = {
      val builder = Seq.newBuilder [TimedCell]
      val loop = new Callback [TimedCell] {
        def pass (cell: TimedCell) {
          builder += cell
          if (iter.hasNext)
            iter.next (this)
        }
        def fail (t: Throwable) = throw t
      }
      if (iter.hasNext)
        iter.next (loop)
      builder.result
    }}

  implicit class RichTimedTable (t: TimedTable) {

    def getAndExpect (key: Bytes, time: TxClock) (expected: TimedCell) {
      val cb = new CallbackCaptor [TimedCell]
      t.get (key, time, cb)
      expectResult (expected) (cb.passed)
    }

    def putAndPass (key: Bytes, time: TxClock, value: Option [Bytes]) {
      val cb = new CallbackCaptor [Unit]
      t.put (key, time, value, cb)
      cb.passed
    }}}
