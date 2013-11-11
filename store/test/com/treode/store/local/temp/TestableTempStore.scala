package com.treode.store.local.temp

import com.treode.concurrent.Callback
import com.treode.pickle.Picklers
import com.treode.store.TableId
import com.treode.store.local.{LocalStore, TableCache, TestableLocalStore, TestableTimedTable}
import com.treode.store.local.locks.LockSpace
import org.scalatest.Assertions

private [local] class TestableTempStore (bits: Int)
extends LocalStore (bits) with TestableLocalStore {

  protected val space = new LockSpace (bits)

  private var _tables = new TableCache [TestableTempTimedTable] {
    def make (id: TableId): TestableTempTimedTable =
      new TestableTempTimedTable
  }

  def table (id: TableId): TestableTimedTable =
    _tables.get (id)
}
