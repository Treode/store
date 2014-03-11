package com.treode.store.atomic

import com.treode.async.{Async, Fiber}
import com.treode.store.{Bytes, TxClock, TxId, WriteOp}
import com.treode.store.tier.TierMedic

import WriteDeputy.ArchiveStatus

private class Medic (val xid: TxId, kit: RecoveryKit)  {
  import kit.{archive, scheduler, tables}

  val fiber = new Fiber (scheduler)

  var _preparing = Option.empty [Seq [WriteOp]]
  var _committed = Option.empty [TxClock]
  var _aborted = false

  def preparing (ops: Seq [WriteOp]): Unit = fiber.execute {
    _preparing = Some (ops)
  }

  def committed (gen: Long, gens: Seq [Long], wt: TxClock): Unit = fiber.execute {
    import ArchiveStatus._
    _committed = Some (wt)
    archive.put (gen, xid.id, Bytes (pickler, Committed (wt)))
    tables.commit (gens, wt, _preparing.get)
  }

  def aborted (gen: Long): Unit = fiber.execute {
    import ArchiveStatus._
    _aborted = true
    archive.put (gen, xid.id, Bytes (pickler, Aborted))
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
