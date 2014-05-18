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

  private val deputies = newWritersMap

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

  def recover (medics: Seq [Medic]): Async [Unit] = {
    for {
      _ <-
        for (m <- medics.latch.unit)
          for (w <- m.close (kit))
            yield deputies.put (m.xid, w)
    } yield ()
  }

  def checkpoint(): Async [Unit] =
    guard {
      for {
        _ <- latch (
            tables.checkpoint(),
            materialize (deputies.values) .latch.unit foreach (_.checkpoint()))
      } yield ()
    }

  def attach () (implicit launch: Disk.Launch) {

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
