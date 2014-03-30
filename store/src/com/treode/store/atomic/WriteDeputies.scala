package com.treode.store.atomic

import com.treode.async.{Async, AsyncImplicits, Latch}
import com.treode.async.misc.materialize
import com.treode.disk.{Disks, ObjectId, PageHandler, Position, RecordDescriptor}
import com.treode.store.{Bytes, TableId, TxId}
import com.treode.store.tier.{TierDescriptor, TierTable}

import Async.{guard, latch, supply}
import AsyncImplicits._

private class WriteDeputies (kit: AtomicKit) extends PageHandler [Long] {
  import WriteDeputy._
  import kit.{archive, cluster, disks, tables}

  private val deputies = newWritersMap

  def get (xid: TxId): WriteDeputy = {
    var d0 = deputies.get (xid.id)
    if (d0 != null)
      return d0
    val d1 = new WriteDeputy (xid, kit)
    d0 = deputies.putIfAbsent (xid.id, d1)
    if (d0 != null)
      return d0
    d1
  }

  def recover (medics: Seq [Medic]): Async [Unit] = {
    for {
      _ <-
        for (m <- medics.latch.unit)
          for (w <- m.close (kit))
            yield deputies.put (m.xid, w)
    } yield ()
  }

  def probe (obj: ObjectId, groups: Set [Long]): Async [Set [Long]] =
    supply (archive.probe (groups))

  def compact (obj: ObjectId, groups: Set [Long]): Async [Unit] =
    guard {
      for {
        meta <- archive.compact (groups)
        _ <- WriteDeputies.checkpoint.record (meta)
      } yield ()
    }

  def checkpoint(): Async [Unit] =
    guard {
      for {
        _ <- latch (
            archive.checkpoint() .flatMap (WriteDeputies.checkpoint.record (_)),
            tables.checkpoint(),
            materialize (deputies.values) .latch.unit foreach (_.checkpoint()))
      } yield ()
    }

  def attach () (implicit launch: Disks.Launch) {

    WriteDeputies.archive.handle (this)

    TimedTable.table.handle (tables)

    prepare.listen { case ((xid, ct, ops), mdtr) =>
      get (xid) .prepare (mdtr, ct, ops)
    }

    commit.listen { case ((xid, wt), mdtr) =>
      get (xid) .commit (mdtr, wt)
    }

    abort.listen { case (xid, mdtr) =>
      get (xid) .abort (mdtr)
    }}}

private object WriteDeputies {

  val checkpoint = {
    import AtomicPicklers._
    RecordDescriptor (0x3D121F46F3D82362L, tierMeta)
  }

  val archive = {
    import AtomicPicklers._
    TierDescriptor (0x36D62E3F7EF580CEL, bytes, const (true))
  }}
