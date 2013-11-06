package com.treode.store.local

import com.treode.concurrent.Callback
import com.treode.store.{Bytes, Value}

trait SimpleTestTools {

  implicit class RichInt (v: Int) {
    def :: (k: Bytes): SimpleCell = SimpleCell (k, Some (Bytes (v)))
  }

  implicit class RichOption (v: Option [Bytes]) {
    def :: (k: Bytes): SimpleCell = SimpleCell (k, v)
  }

  implicit class RichCellIterator (iter: SimpleIterator) {

    def toSeq: Seq [SimpleCell] = {
      val builder = Seq.newBuilder [SimpleCell]
      val loop = new Callback [SimpleCell] {
        def pass (cell: SimpleCell) {
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
