package com.treode.store.atomic

import com.treode.async.{Async, Callback, Fiber}
import com.treode.store.{TxClock, TxId, WriteOp}

import Async.when

private class Medic (val xid: TxId, kit: RecoveryKit)  {
  import kit.{scheduler, tables}

  val fiber = new Fiber

  var _preparing = Option.empty [Seq [WriteOp]]
  var _committed = Option.empty [TxClock]
  var _aborted = false

  def preparing (ops: Seq [WriteOp]): Unit = fiber.execute {
    _preparing = Some (ops)
  }

  def committed (gens: Seq [Long], wt: TxClock): Unit = fiber.execute {
    _committed = Some (wt)
    tables.commit (gens, wt, _preparing.get)
  }

  def aborted(): Unit = fiber.execute {
    _aborted = true
  }

  def close (kit: AtomicKit): Async [WriteDeputy] =
    fiber.guard {
      val w = new WriteDeputy (xid, kit)
      for {
        _ <- when (_committed.isDefined) (w.commit (_committed.get))
        _ <- when (_aborted) (w.abort())
        _ <- when (_preparing.isDefined) (w.prepare (TxClock.zero, _preparing.get))
      } yield w
    }}
