package com.treode.store.atomic

import com.treode.async.Async
import com.treode.async.misc.materialize
import com.treode.disk.Disks
import com.treode.store.{TableId, TxClock, WriteOp}
import com.treode.store.tier.{TierMedic, TierTable}

import Async.guard
import TimedTable.keyToBytes

private class TimedMedic (kit: RecoveryKit) {
  import kit.{config, scheduler}

  val tables = newTableMedicsMap

  def get (id: TableId): TierMedic = {
    val m1 = TierMedic (TimedTable.table, id.id)
    val m0 = tables.putIfAbsent (id, m1)
    if (m0 == null) m1 else m0
  }

  def commit (gens: Seq [Long], wt: TxClock, ops: Seq [WriteOp]) {
    import WriteOp._
    require (gens.length == ops.length)
    for ((gen, op) <- gens zip ops) {
      val t = get (op.table)
      val k = keyToBytes (op.key, wt)
      op match {
        case op: Create => t.put (gen, k, op.value)
        case op: Hold   => 0
        case op: Update => t.put (gen, k, op.value)
        case op: Delete => t.delete (gen, k)
      }}}

  def checkpoint (id: TableId, meta: TierTable.Meta): Unit =
    get (id) .checkpoint (meta)

  def close() (implicit launch: Disks.Launch): Seq [(TableId, TimedTable)] = {
    materialize (tables.entrySet) map { entry =>
      val id = entry.getKey
      val t = new TimedTable (entry.getValue.close())
      (id, t)
    }}
}