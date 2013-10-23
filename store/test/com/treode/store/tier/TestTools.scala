package com.treode.store.tier

import com.treode.concurrent.Callback
import com.treode.store.{Bytes, TxClock, Value}

trait TestTools {

  implicit class RichBytes (v: Bytes) {
    def ## (time: Int) = Cell (v, TxClock (time), None)
  }

  implicit class RichInt (v: Int) {
    def :: (cell: Cell): Cell = Cell (cell.key, cell.time, Some (Bytes (v)))
    def :: (time: Int): Value = Value (time, Some (Bytes (v)))
  }

  implicit class RichOption (v: Option [Bytes]) {
    def :: (time: Int): Value = Value (time, v)
  }

  implicit class RichCellIterator (iter: CellIterator) {

    def toSeq: Seq [Cell] = {
      val builder = Seq.newBuilder [Cell]
      val loop = new Callback [Cell] {
        def pass (cell: Cell) {
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
