package com.treode.store.temp

import com.treode.store._
import org.scalatest.Assertions

import Assertions.expectResult

private [store] class TestableTempKit (implicit config: StoreConfig)
extends LocalKit with TestableLocalKit {

  private val timedTables = new TableCache [TestableTempTimedTable] {
    def make (id: TableId) = new TestableTempTimedTable
  }

  def getTimedTable (id: TableId): TimedTable =
    timedTables.get (id)

  def expectCells (id: TableId) (cs: TimedCell*): Unit =
    expectResult (cs) (timedTables.get (id) .toSeq)

  def close() = ()
}
