package com.treode.store.atomic

import com.treode.async.Async
import com.treode.async.misc.materialize
import com.treode.disk.Disk
import com.treode.store.{TableId, TxClock, WriteOp}
import com.treode.store.tier.{TierMedic, TierTable}

import Async.guard

private class TimedMedic (kit: RecoveryKit) {
  import kit.{config, scheduler}

  val tables = newTableMedicsMap

  def get (id: TableId): TierMedic = {
    val m1 = TierMedic (TimedStore.table, id.id)
    val m0 = tables.putIfAbsent (id, m1)
    if (m0 == null) m1 else m0
  }

  def commit (gens: Seq [Long], wt: TxClock, ops: Seq [WriteOp]) {
    import WriteOp._
    require (gens.length == ops.length)
    for ((gen, op) <- gens zip ops) {
      val t = get (op.table)
      op match {
        case op: Create => t.put (gen, op.key, wt, op.value)
        case op: Hold   => 0
        case op: Update => t.put (gen, op.key, wt, op.value)
        case op: Delete => t.delete (gen, op.key, wt)
      }}}

  def checkpoint (id: TableId, meta: TierTable.Meta): Unit =
    get (id) .checkpoint (meta)

  def close() (implicit launch: Disk.Launch): Seq [(TableId, TierTable)] = {
    materialize (tables.entrySet) map { entry =>
      val id = entry.getKey
      val t = entry.getValue.close()
      (id, t)
    }}
}
