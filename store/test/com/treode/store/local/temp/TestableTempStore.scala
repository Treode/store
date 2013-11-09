package com.treode.store.local.temp

import com.treode.concurrent.Callback
import com.treode.pickle.Picklers
import com.treode.store.TableId
import com.treode.store.local.{TestableLocalStore, TestableTimedTable}
import com.treode.store.local.locks.LockSpace
import org.scalatest.Assertions

private [store] class TestableTempStore (bits: Int) extends TestableLocalStore {

  protected val space = new LockSpace (bits)

  private var _tables = Map.empty [TableId, TestableTempTimedTable]

  def table (id: TableId): TestableTimedTable = synchronized {
    _tables.get (id) match {
      case Some (t) =>
        t
      case None =>
        val t = new TestableTempTimedTable
        _tables += (id -> t)
        t
    }}}
