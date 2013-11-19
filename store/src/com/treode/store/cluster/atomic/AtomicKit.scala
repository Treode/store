package com.treode.store.cluster.atomic

import java.util.concurrent.ConcurrentHashMap

import com.treode.cluster.Host
import com.treode.concurrent.Callback
import com.treode.store._
import com.treode.store.cluster.paxos.PaxosKit

private class AtomicKit (implicit val host: Host, val store: LocalStore, val paxos: PaxosStore) {

  object Deputies {
    import Deputy._

    private val deputies = new ConcurrentHashMap [Bytes, Deputy] ()

    val mainDb = {
      import AtomicPicklers._
      SimpleAccessor.value (store, 0x5BA844914406891FL, deputyStatus)
    }

    val openDb = {
      import AtomicPicklers._
      SimpleAccessor.value (store, 0x9C5E5712B0C36CA8L, unit)
    }

    def get (xid: TxId): Deputy = {
      var d0 = deputies.get (xid.id)
      if (d0 != null)
        return d0
      val d1 = new Deputy (xid, AtomicKit.this)
      d0 = deputies.putIfAbsent (xid.id, d1)
      if (d0 != null)
        return d0
      d1
    }

    prepare.register { case ((xid, ct, ops), mdtr) =>
      get (xid) .prepare (mdtr, ct, ops)
    }

    commit.register { case ((xid, wt), mdtr) =>
      get (xid) .commit (mdtr, wt)
    }

    abort.register { case (xid, mdtr) =>
      get (xid) .abort (mdtr)
    }}

  Deputies

  def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback): Unit =
    Callback.guard (cb) {
      new Director (xid, ct, ops, this) .open (cb)
    }

  def close() {
    Deputies.mainDb.close()
    Deputies.openDb.close()
  }}
