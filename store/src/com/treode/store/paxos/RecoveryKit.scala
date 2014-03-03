package com.treode.store.paxos

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.Cluster
import com.treode.cluster.misc.materialize
import com.treode.disk.Disks
import com.treode.store.{Bytes, Paxos, StoreConfig}
import com.treode.store.catalog.CohortCatalog
import com.treode.store.tier.TierMedic

import Acceptors.{active, root}
import Acceptor.{ActiveStatus, open, promise, accept, reaccept, close}

private class RecoveryKit (implicit
    val random: Random,
    val scheduler: Scheduler,
    val cluster: Cluster,
    val recovery: Disks.Recovery,
    val config: StoreConfig
) extends Paxos.Recovery {

  val archive = TierMedic (Acceptors.archive)
  val medics = newMedicsMap

  def openByStatus (status: ActiveStatus) {
    val m1 = Medic (status, this)
    val m0 = medics.putIfAbsent (m1.key, m1)
    require (m0 == null, s"Already recovering paxos instance ${m1.key}.")
  }

  def openWithDefault (key: Bytes, default: Bytes) {
    var m0 = medics.get (key)
    if (m0 != null)
      return
    val m1 = Medic (key, default, this)
    m0 = medics.putIfAbsent (key, m1)
    if (m0 != null)
      return
  }

  def get (key: Bytes): Medic = {
    val m = medics.get (key)
    require (m != null, s"Exepcted to be recovering paxos instance $key.")
    m
  }

  root.reload { root => implicit reload =>
    archive.checkpoint (root.archive)
    for (ss <- active.read (reload, root.active))
      yield (ss foreach openByStatus)
  }

  open.replay { case (key, default) =>
    openWithDefault (key, default)
  }

  promise.replay { case (key, ballot) =>
    get (key) promised (ballot)
  }

  accept.replay { case (key, ballot, value) =>
    get (key) accepted (ballot, value)
  }

  reaccept.replay { case (key, ballot) =>
    get (key) reaccepted (ballot)
  }

  close.replay { case (key, chosen, gen) =>
    get (key) closed (chosen, gen)
  }

  def launch (implicit launch: Disks.Launch, cohorts: CohortCatalog): Async [Paxos] = {
    import launch.disks

    val kit = new PaxosKit (archive.close())
    import kit.{acceptors, proposers}

    root.checkpoint (acceptors.checkpoint())

    for {
      _ <- acceptors.recover (materialize (medics.values))
    } yield {
      acceptors.attach()
      proposers.attach()
      kit
    }}}
