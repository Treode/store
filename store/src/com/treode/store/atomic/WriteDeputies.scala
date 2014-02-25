package com.treode.store.atomic

import java.util.concurrent.ConcurrentHashMap

import com.treode.disk.{PageDescriptor, Position, RootDescriptor}
import com.treode.store.{Bytes, TxId}
import com.treode.store.tier.{TierDescriptor, TierTable}

private class WriteDeputies (val db: TierTable, kit: AtomicKit) {
  import WriteDeputy._
  import kit.cluster

  private val deputies = new ConcurrentHashMap [Bytes, WriteDeputy] ()

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

  class Root (val pos: Position, val db: TierTable.Meta)

  object Root {

    val pickler = {
      import AtomicPicklers._
      wrap (pos, tierMeta)
      .build (v => new Root (v._1, v._2))
      .inspect (v => (v.pos, v.db))
    }}

  val root = {
    import AtomicPicklers._
    RootDescriptor (0xBFD4F3D3, Root.pickler)
  }

  val pager = {
    import AtomicPicklers._
    PageDescriptor (0x7C71E2AF, const (0), seq (activeStatus))
  }

  val db = {
    import AtomicPicklers._
    TierDescriptor (0xB601F6C0, bytes, const (true))
  }}
