package com.treode.store.local

import scala.util.Random

import com.treode.concurrent.{Callback, CallbackCaptor}
import com.treode.store.{TimedCell, TimedTestTools}
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

  def expectCells (cs: TimedCell*) (actual: TestableTimedTable) =
    expectResult (cs) (actual.toSeq)
}
