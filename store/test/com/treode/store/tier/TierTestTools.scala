package com.treode.store.tier

import com.treode.async.{Async, StubScheduler}
import com.treode.store.{Bytes, Cell, Fruits, TimedTestTools, TxClock}
import org.scalatest.Assertions

import Assertions.assertResult
import Async.async
import Fruits.{Apple, Tomato}

private object TierTestTools extends TimedTestTools {

  implicit class RichTierTable (table: TierTable) (implicit scheduler: StubScheduler) {

    def putCells (cells: Cell*) {
      for (Cell (key, time, value) <- cells)
        table.put (key, time, value.get)
      scheduler.runTasks()
    }

    def deleteCells (cells: Cell*) {
      for (Cell (key, time, _) <- cells)
        table.delete (key, time)
      scheduler.runTasks()
    }

    /** Requires that
      *   - cells are in order
      *   - cell times have gaps
      *   - keys are between Apple and Tomato, exclusive
      */
    def check (cells: Cell*) {
      for (Seq (c1, c2) <- cells.sliding (2)) {
        require (c1 < c2, "Cells must be in order.")
        require (c1.key != c2.key || c1.time > c2.time+1, s"Times must have gaps.")
        require (Apple < c1.key && c1.key < Tomato, "Key must be between Apple and Tomato.")
      }
      assertResult (cells) (table.iterator.toSeq)
      table.ceiling (Apple, 0) .expect (Apple##0)
      table.ceiling (Tomato, 0) .expect (Tomato##0)
      for (Seq (c1, c2) <- cells.sliding (2)) {
        table.ceiling (c1.key, c1.time + 1) .expect (c1)
        table.ceiling (c1.key, c1.time) .expect (c1)
        if (c1.key == c2.key)
          table.ceiling (c1.key, c1.time - 1) .expect (c2)
        else
          table.ceiling (c1.key, c1.time - 1) .expect (Cell (c1.key, 0, None))
      }}}}
