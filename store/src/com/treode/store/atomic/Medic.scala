package com.treode.store.atomic

import com.treode.async.{Async, Fiber}
import com.treode.store.{TxClock, TxId, WriteOp}

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
    fiber.supply {
      val void = WriteDeputy.prepare.void
      val w = new WriteDeputy (xid, kit)
      _committed match {
        case Some (wt) => w.commit (void, wt)
        case None => ()
      }
      _aborted match {
        case true => w.abort (void)
        case fase => ()
      }
      _preparing match {
        case Some (ops) => w.prepare (void, TxClock.zero, ops)
        case None => ()
      }
      w
    }}
