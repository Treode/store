package com.treode.store.atomic

import java.util.concurrent.ConcurrentHashMap
import scala.util.Random

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Disks
import com.treode.store._
import com.treode.store.paxos.Paxos
import com.treode.store.tier.TierTable

import Async.async

private class AtomicKit (
    db: TierTable
) (implicit
    val random: Random,
    val scheduler: Scheduler,
    val cluster: Cluster,
    val disks: Disks,
    val paxos: Paxos,
    val config: StoreConfig
) extends Store {

  val store = new TimedStore (this)
  val reader = new ReadDeputy (this)
  val writers = new WriteDeputies (db, this)

  def read (rt: TxClock, ops: Seq [ReadOp]): Async [Seq [Value]] =
    async (new ReadDirector (rt, ops, this, _))

  private def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp], cb: Callback [WriteResult]): Unit =
    cb.defer {
      new WriteDirector (xid, ct, ops, this) .open (cb)
    }

  def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp]): Async [WriteResult] =
    async (write (xid, ct, ops, _))

  def attach() {
    reader.attach()
    writers.attach()
  }

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
      config: StoreConfig
  ): Recovery =
    new RecoveryKit
}
