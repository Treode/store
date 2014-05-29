package com.treode.store.atomic

import com.treode.async.Async
import com.treode.async.implicits._
import com.treode.async.misc.materialize
import com.treode.disk.Disk
import com.treode.store.TxId

import Async.{guard, latch}

private class WriteDeputies (kit: AtomicKit) {
  import WriteDeputy._
  import kit.{cluster, tables}

  val deputies = newWritersMap

  def get (xid: TxId): WriteDeputy = {
    var d0 = deputies.get (xid.id)
    if (d0 != null)
      return d0
    val d1 = new WriteDeputy (xid, kit)
    d0 = deputies.putIfAbsent (xid, d1)
    if (d0 != null)
      return d0
    d1
  }

  def remove (xid: TxId, w: WriteDeputy): Unit =
    deputies.remove (xid, w)

  def recover (medics: Iterable [Medic]) {
    for (m <- medics)
      deputies.put (m.xid, m.close (kit))
  }

  def checkpoint(): Async [Unit] =
    guard {
      for {
        _ <- materialize (deputies.values) .latch.unit foreach (_.checkpoint())
        _ <- tables.checkpoint()
      } yield ()
    }

  def attach () (implicit launch: Disk.Launch) {

    launch.checkpoint (checkpoint())

    TimedStore.table.handle (tables)

    prepare.listen { case ((xid, ct, ops), from) =>
      get (xid) .prepare (ct, ops)
    }

    commit.listen { case ((xid, wt), from) =>
      get (xid) .commit (wt)
    }

    abort.listen { case (xid, from) =>
      get (xid) .abort()
    }}}
