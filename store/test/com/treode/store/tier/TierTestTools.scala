package com.treode.store.tier

import com.treode.async.{Async, Scheduler}
import com.treode.async.stubs.StubScheduler
import com.treode.store.{Bytes, Cell, Fruits, Residents, StoreTestTools, TxClock}
import org.scalatest.Assertions

import Assertions.assertResult
import Async.async
import Fruits.{Apple, Tomato}
import TierTable.Meta

private object TierTestTools extends StoreTestTools {

  implicit class RichTierTable (table: TierTable) {

    def putCells (cells: Cell*) (implicit scheduler: StubScheduler) {
      for (Cell (key, time, value) <- cells)
        table.put (key, time, value.get)
      scheduler.runTasks()
    }

    def deleteCells (cells: Cell*) (implicit scheduler: StubScheduler) {
      for (Cell (key, time, _) <- cells)
        table.delete (key, time)
      scheduler.runTasks()
    }

    def checkpoint(): Async [Meta] =
      table.checkpoint (Residents.all)

    /** Requires that
      *   - cells are in order
      *   - cell times have gaps
      *   - keys are between Apple and Tomato, exclusive
      */
    def check (cells: Cell*) (implicit scheduler: StubScheduler) {
      for (Seq (c1, c2) <- cells.sliding (2)) {
        require (c1 < c2, "Cells must be in order.")
        require (c1.key != c2.key || c1.time > c2.time+1, s"Times must have gaps.")
        require (Apple < c1.key && c1.key < Tomato, "Key must be between Apple and Tomato.")
      }
      assertResult (cells) (table .iterator (Residents.all) .toSeq)
      table.get (Apple, 0) .expect (Apple##0)
      table.get (Tomato, 0) .expect (Tomato##0)
      for (Seq (c1, c2) <- cells.sliding (2)) {
        table.get (c1.key, c1.time + 1) .expect (c1)
        table.get (c1.key, c1.time) .expect (c1)
        if (c1.key == c2.key)
          table.get (c1.key, c1.time - 1) .expect (c2)
        else
          table.get (c1.key, c1.time - 1) .expect (Cell (c1.key, 0, None))
      }}}}
