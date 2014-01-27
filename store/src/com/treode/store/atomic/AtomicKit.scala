package com.treode.store.atomic

import java.util.concurrent.ConcurrentHashMap
import scala.util.Random

import com.treode.async.{Scheduler, guard}
import com.treode.cluster.Cluster
import com.treode.store._
import com.treode.store.paxos.Paxos

private class AtomicKit (implicit val random: Random, val scheduler: Scheduler,
    val cluster: Cluster, val store: LocalStore, val paxos: Paxos) extends Store {

  object ReadDeputies {

    val deputy = new ReadDeputy (AtomicKit.this)

    ReadDeputy.read.listen { case ((rt, ops), mdtr) =>
      deputy.read (mdtr, rt, ops)
    }}

  object WriteDeputies {
    import WriteDeputy._

    private val deputies = new ConcurrentHashMap [Bytes, WriteDeputy] ()

    def get (xid: TxId): WriteDeputy = {
      var d0 = deputies.get (xid.id)
      if (d0 != null)
        return d0
      val d1 = new WriteDeputy (xid, AtomicKit.this)
      d0 = deputies.putIfAbsent (xid.id, d1)
      if (d0 != null)
        return d0
      d1
    }

    prepare.listen { case ((xid, ct, ops), mdtr) =>
      get (xid) .prepare (mdtr, ct, ops)
    }

    commit.listen { case ((xid, wt), mdtr) =>
      get (xid) .commit (mdtr, wt)
    }

    abort.listen { case (xid, mdtr) =>
      get (xid) .abort (mdtr)
    }}

  ReadDeputies
  WriteDeputies

  def read (rt: TxClock, ops: Seq [ReadOp], cb: ReadCallback): Unit =
    guard (cb) {
      new ReadDirector (rt, ops, this, cb)
    }

  def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp], cb: WriteCallback): Unit =
    guard (cb) {
      new WriteDirector (xid, ct, ops, this) .open (cb)
    }

  def close() = ()
}
