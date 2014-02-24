package com.treode.store.atomic

import java.util.concurrent.ConcurrentHashMap
import scala.util.Random

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Disks
import com.treode.store._
import com.treode.store.paxos.Paxos

import Async.async

private class AtomicKit (detector: Int) (implicit
    val random: Random,
    val scheduler: Scheduler,
    val cluster: Cluster,
    val store: LocalStore,
    val paxos: Paxos
) extends Store {

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

  private def read (rt: TxClock, ops: Seq [ReadOp], cb: ReadCallback): Unit =
    cb.defer {
      new ReadDirector (rt, ops, this, cb)
    }

  def read (rt: TxClock, ops: Seq [ReadOp]): Async [Seq [Value]] =
    async { cb =>
      read (rt, ops, new ReadCallback {
        def pass (vs: Seq [Value]) = cb.pass (vs)
        def fail (t: Throwable) = cb.fail (t)
      })
    }

  private def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp], cb: Callback [WriteResult]): Unit =
    cb.defer {
      new WriteDirector (xid, ct, ops, this) .open (cb)
    }

  def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp]): Async [WriteResult] =
    async (write (xid, ct, ops, _))

  def close() = ()
}

private object AtomicKit {

  trait Recovery {
    def launch (implicit launch: Disks.Launch, paxos: Paxos): Async [AtomicKit]
  }

  def recover() (implicit
      random: Random,
      scheduler: Scheduler,
      cluster: Cluster,
      recover: Disks.Recovery,
      store: LocalStore,
      config: StoreConfig
  ): Recovery =
    new RecoveryKit
}
