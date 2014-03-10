package com.treode.store.atomic

import com.treode.async.{Async, AsyncConversions, Latch}
import com.treode.async.misc.materialize
import com.treode.disk.{PageDescriptor, Position, RootDescriptor}
import com.treode.store.{Bytes, TableId, TxId}
import com.treode.store.tier.{TierDescriptor, TierTable}

import Async.guard
import AsyncConversions._
import WriteDeputies.Root

private class WriteDeputies (kit: AtomicKit) {
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
      _ <- medics.latch.unit { m =>
        for (w <- m.close (kit))
          yield deputies.put (m.xid, w)
      }
    } yield ()
  }

  def checkpoint(): Async [Root] =
    guard {
      val _deputies = materialize (deputies.values)
      for {
        (__active, __tables, _archive) <- Latch.triple (
            _deputies.latch.seq (_.checkpoint()),
            tables.checkpoint(),
            archive.checkpoint())
        (_active, _tables) <- Latch.pair (
            WriteDeputies.active.write (0, 0, __active.flatten),
            TimedStore.tables.write (0, 0, __tables))
      } yield new Root (_active, _archive, _tables)
    }

  def attach() {

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

  class Root (
      val active: Position,
      val archive: TierTable.Meta,
      val tables: Position)

  object Root {

    val pickler = {
      import AtomicPicklers._
      wrap (pos, tierMeta, pos)
      .build (v => new Root (v._1, v._2, v._3))
      .inspect (v => (v.active, v.archive, v.tables))
    }}

  val root = {
    import AtomicPicklers._
    RootDescriptor (0xB0E4265A9A70F753L, Root.pickler)
  }

  val active = {
    import AtomicPicklers._
    PageDescriptor (0x86CA87954DF61878L, const (0), seq (activeStatus))
  }

  val archive = {
    import AtomicPicklers._
    TierDescriptor (0x36D62E3F7EF580CEL, bytes, const (true))
  }}
