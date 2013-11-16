package com.treode.store.local.temp

import com.treode.store.{SimpleTable, LocalStore, TableId}
import com.treode.store.local.{LocalKit, TableCache, TestableLocalKit, TestableTimedTable}

private [local] class TestableTempKit (bits: Int) extends LocalKit (bits) with TestableLocalKit {

  private val tables = new TableCache [TestableTimedTable] {
    def make (id: TableId) = new TestableTempTimedTable
  }

  def getTimedTable (id: TableId): TestableTimedTable =
    tables.get (id)

  def openSimpleTable (id: TableId): SimpleTable =
    new TempSimpleTable
}

private [store] object TestableTempKit {

  def apply (bits: Int): LocalStore =
    new TestableTempKit (bits)
}
