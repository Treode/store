package com.treode.store.paxos

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.misc.materialize
import com.treode.cluster.Cluster
import com.treode.disk.Disks
import com.treode.store.{Atlas, Bytes, Paxos, StoreConfig}
import com.treode.store.tier.TierMedic

import Acceptors.Root
import Acceptor.{ActiveStatus, open, promise, accept, reaccept, close}
import Async.cond

private class RecoveryKit (implicit
    val random: Random,
    val scheduler: Scheduler,
    val cluster: Cluster,
    val recovery: Disks.Recovery,
    val config: StoreConfig
) extends Paxos.Recovery {

  val archive = TierMedic (Acceptors.archive, 0)
  val medics = newMedicsMap
  var root = Option.empty [Root]

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

  Acceptors.root.reload { root =>
    this.root = Some (root)
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

  def checkpoint (status: ActiveStatus) {
    val m1 = Medic (status, this)
    val m0 = medics.putIfAbsent (m1.key, m1)
    if (m0 != null)
      m0.checkpoint (status)
  }

  def checkpoint (root: Root) (implicit disks: Disks): Async [Unit] = {
    for {
      _active <- Acceptors.active.read (root.active)
    } yield {
      _active foreach checkpoint
    }}

  def launch (implicit launch: Disks.Launch, atlas: Atlas): Async [Paxos] = {
    import launch.disks

    val kit = new PaxosKit (archive.close())
    import kit.{acceptors, proposers}

    Acceptors.root.checkpoint (acceptors.checkpoint())

    for {
      _ <- cond (root.isDefined) (checkpoint (root.get))
      _ <- acceptors.recover (materialize (medics.values))
    } yield {
      acceptors.attach()
      proposers.attach()
      kit
    }}}
