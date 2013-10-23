package com.treode.store.simple

import com.treode.concurrent.Callback
import com.treode.store.{Bytes, Value}

trait TestTools {

  implicit class RichInt (v: Int) {
    def :: (k: Bytes): Cell = Cell (k, Some (Bytes (v)))
  }

  implicit class RichOption (v: Option [Bytes]) {
    def :: (k: Bytes): Cell = Cell (k, v)
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
