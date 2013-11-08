package com.treode.store.local

import com.treode.concurrent.Callback
import com.treode.pickle.Picklers
import com.treode.store._
import com.treode.store.local.locks.LockSpace
import org.scalatest.Assertions

private class TestableTempLocalStore (bits: Int) extends TestableLocalStore {

  protected val space = new LockSpace (bits)

  private var _tables = Map.empty [TableId, TestableTempTimedTable]

  def table (id: TableId): TestableTempTimedTable = synchronized {
    _tables.get (id) match {
      case Some (t) =>
        t
      case None =>
        val t = new TestableTempTimedTable
        _tables += (id -> t)
        t
    }}}
