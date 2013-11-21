package com.treode.store.local

import com.treode.async.Callback
import com.treode.store.{SimpleCell, SimpleTestTools}

private object LocalSimpleTestTools extends SimpleTestTools {

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
    }}}
