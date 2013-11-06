package com.treode.store.local

import com.treode.concurrent.Callback
import com.treode.store.{Bytes, TxClock, Value}

trait TimedTestTools {

  implicit class RichBytes (v: Bytes) {
    def ## (time: Int) = TimedCell (v, TxClock (time), None)
  }

  implicit class RichInt (v: Int) {
    def :: (cell: TimedCell): TimedCell = TimedCell (cell.key, cell.time, Some (Bytes (v)))
    def :: (time: Int): Value = Value (time, Some (Bytes (v)))
  }

  implicit class RichOption (v: Option [Bytes]) {
    def :: (time: Int): Value = Value (time, v)
  }

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
}
