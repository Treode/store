package com.treode.store.atomic

import scala.util.Random

import com.treode.async.{Async, Latch, Scheduler}
import com.treode.async.misc.materialize
import com.treode.cluster.Cluster
import com.treode.disk.Disks
import com.treode.store.{Atlas, Paxos, Store, StoreConfig, TxId}
import com.treode.store.tier.TierMedic

import WriteDeputy.{aborted, committed, preparing}

private class RecoveryKit (implicit
    val random: Random,
    val scheduler: Scheduler,
    val cluster: Cluster,
    val recovery: Disks.Recovery,
    val config: StoreConfig
) extends AtomicKit.Recovery {

  val archive = TierMedic (WriteDeputies.archive, 0)
  val tables = new TimedMedic (this)
  val writers = newWriterMedicsMap

  def get (xid: TxId): Medic = {
    val m1 = new Medic (xid, this)
    val m0 = writers.putIfAbsent (m1.xid, m1)
    if (m0 == null) m1 else m0
  }

  preparing.replay { case (xid, ops) =>
    get (xid) .preparing (ops)
  }

  committed.replay { case (xid, gen, gens, wt) =>
    get (xid) .committed (gen, gens, wt)
  }

  aborted.replay { case (xid, gen) =>
    get (xid) .aborted (gen)
  }

  TimedStore.checkpoint.replay { case (tab, meta) =>
    tables.checkpoint (tab, meta)
  }

  WriteDeputies.checkpoint.replay { case meta =>
    archive.checkpoint (meta)
  }

  def launch (implicit launch: Disks.Launch, atlas: Atlas, paxos: Paxos): Async [Store] = {
    import launch.disks

    val kit = new AtomicKit (archive.close())
    kit.tables.recover (tables.close())

    for {
      _ <- kit.writers.recover (materialize (writers.values))
    } yield {
      kit.reader.attach()
      kit.writers.attach()
      kit.rebalancer.attach()
      kit
    }}}
