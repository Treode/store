package com.treode.store.atomic

import scala.collection.JavaConversions
import scala.util.Random

import com.treode.async.{Async, Latch, Scheduler}
import com.treode.async.misc.materialize
import com.treode.cluster.Cluster
import com.treode.disk.Disk
import com.treode.store.{Cohort, Library, Store, StoreConfig, TxId}
import com.treode.store.paxos.Paxos
import com.treode.store.tier.TierMedic

import Async.supply
import JavaConversions._
import TimedStore.{receive, checkpoint}
import WriteDeputy.{aborted, committed, preparing}

private class RecoveryKit (implicit
    val random: Random,
    val scheduler: Scheduler,
    val cluster: Cluster,
    val library: Library,
    val recovery: Disk.Recovery,
    val config: StoreConfig
) extends AtomicKit.Recovery {

  val tables = new TimedMedic (this)
  val writers = newWriterMedicsMap

  def get (xid: TxId): Medic = {
    var m0 = writers.get (xid)
    if (m0 != null) return m0
    val m1 = new Medic (xid, this)
    m0 = writers.putIfAbsent (xid, m1)
    if (m0 != null) return m0
    m1
  }

  preparing.replay { case (xid, ops) =>
    get (xid) .preparing (ops)
  }

  committed.replay { case (xid, gens, wt) =>
    get (xid) .committed (gens, wt)
  }

  aborted.replay { xid =>
    get (xid) .aborted()
  }

  receive.replay { case (tab, gen, novel) =>
    tables.receive (tab, gen, novel)
  }

  checkpoint.replay { case (tab, meta) =>
    tables.checkpoint (tab, meta)
  }

  def launch (implicit launch: Disk.Launch, paxos: Paxos): Async [Store] =
    supply {
      import launch.disks

      val kit = new AtomicKit()
      kit.tables.recover (tables.close())
      kit.writers.recover (writers.values)
      kit.reader.attach()
      kit.writers.attach()
      kit.scanner.attach()
      kit
    }}
