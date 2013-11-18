package com.treode.store.local.temp

import com.treode.store.{SimpleTable, TableId, TestableLocalStore, TimedCell}
import com.treode.store.local.{LocalKit, TableCache, TestableLocalKit, TimedTable}
import org.scalatest.Assertions

import Assertions.expectResult

private [local] class TestableTempKit (bits: Int) extends LocalKit (bits) with TestableLocalKit {

  private val timedTables = new TableCache [TestableTempTimedTable] {
    def make (id: TableId) = new TestableTempTimedTable
  }

  def getTimedTable (id: TableId): TimedTable =
    timedTables.get (id)

  def expectCells (id: TableId) (cs: TimedCell*): Unit =
    expectResult (cs) (timedTables.get (id) .toSeq)

  def openSimpleTable (id: TableId): SimpleTable =
    new TempSimpleTable

  def close() = ()
}

private [store] object TestableTempKit {

  def apply (bits: Int): TestableLocalStore =
    new TestableTempKit (bits)
}
