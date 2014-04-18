package com.treode.store.atomic

import scala.util.Random

import com.treode.async.{Async, AsyncImplicits, Callback, Scheduler}
import com.treode.cluster.{Cluster, ReplyTracker}
import com.treode.disk.Disks
import com.treode.store._
import com.treode.store.tier.TierTable

import Async.{async, supply}
import AsyncImplicits._
import Rebalancer.Targets

private class AtomicKit (implicit
    val random: Random,
    val scheduler: Scheduler,
    val cluster: Cluster,
    val disks: Disks,
    val atlas: Atlas,
    val paxos: Paxos,
    val config: StoreConfig
) extends Store {

  val tables = new TimedStore (this)
  val reader = new ReadDeputy (this)
  val writers = new WriteDeputies (this)
  val rebalancer = new Rebalancer (this)

  def locate (table: TableId, key: Bytes): Cohort =
    atlas.locate ((table, key).hashCode)

  def locate (op: Op): Cohort =
    locate (op.table, op.key)

  def read (rt: TxClock, ops: Seq [ReadOp]): Async [Seq [Value]] =
    async (new ReadDirector (rt, ops, this, _))

  private def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp], cb: Callback [WriteResult]): Unit =
    cb.defer {
      new WriteDirector (xid, ct, ops, this) .open (cb)
    }

  def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp]): Async [WriteResult] =
    async (write (xid, ct, ops, _))

  def rebalance (cohorts: Array [Cohort]): Async [Unit] = {
    val targets = Targets (cohorts)
    for {
      _ <- rebalancer.rebalance (targets)
    } yield {
      if (targets.isEmpty)
        tables.compact()
    }}}

private [store] object AtomicKit {

  trait Recovery {

    def launch (implicit launch: Disks.Launch, atlas: Atlas, paxos: Paxos): Async [Store]
  }

  def recover() (implicit
      random: Random,
      scheduler: Scheduler,
      cluster: Cluster,
      recovery: Disks.Recovery,
      config: StoreConfig
  ): Recovery =
    new RecoveryKit
}
