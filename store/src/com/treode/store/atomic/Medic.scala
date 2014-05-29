package com.treode.store.atomic

import com.treode.async.{Async, Callback, Fiber}
import com.treode.store.{TxClock, TxId, WriteOp}

import Callback.ignore

private class Medic (val xid: TxId, kit: RecoveryKit)  {
  import kit.tables

  var _preparing = Option.empty [Seq [WriteOp]]
  var _committed = Option.empty [TxClock]
  var _aborted = false

  def preparing (ops: Seq [WriteOp]): Unit = synchronized {
    _preparing = Some (ops)
  }

  def committed (gens: Seq [Long], wt: TxClock): Unit = synchronized {
    _committed = Some (wt)
  }

  def aborted(): Unit = synchronized {
    _aborted = true
  }

  def close (kit: AtomicKit): WriteDeputy = synchronized {
    val w = new WriteDeputy (xid, kit)
    if (_committed.isDefined)
      w.commit (_committed.get) .run (ignore)
    if (_aborted)
      w.abort() .run (ignore)
    if (_preparing.isDefined)
      w.prepare (TxClock.zero, _preparing.get) run (ignore)
    w
  }

  override def toString = s"WriteDeputy.Medic($xid)"
}
