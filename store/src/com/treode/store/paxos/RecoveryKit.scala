package com.treode.store.paxos

import scala.collection.JavaConversions
import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.implicits._
import com.treode.async.misc.materialize
import com.treode.cluster.Cluster
import com.treode.disk.Disks
import com.treode.store.{Bytes, Library, StoreConfig, TxClock}
import com.treode.store.tier.TierMedic

import Async.supply
import Acceptors.checkpoint
import Acceptor.{open, promise, accept, reaccept, close}
import JavaConversions._

private class RecoveryKit (implicit
    val random: Random,
    val scheduler: Scheduler,
    val cluster: Cluster,
    val library: Library,
    val recovery: Disks.Recovery,
    val config: StoreConfig
) extends Paxos.Recovery {

  val archive = TierMedic (Acceptors.archive, 0)
  val medics = newMedicsMap

  def openWithDefault (key: Bytes, time: TxClock, default: Bytes) {
    var m0 = medics.get ((key, time))
    if (m0 != null)
      return
    val m1 = Medic (key, time, default, this)
    m0 = medics.putIfAbsent ((key, time), m1)
    if (m0 != null)
      return
  }

  def get (key: Bytes, time: TxClock): Medic = {
    val m = medics.get ((key, time))
    require (m != null, s"Exepcted to be recovering paxos instance $key:$time.")
    m
  }

  open.replay { case (key, time, default) =>
    openWithDefault (key, time, default)
  }

  promise.replay { case (key, time, ballot) =>
    get (key, time) promised (ballot)
  }

  accept.replay { case (key, time, ballot, value) =>
    get (key, time) accepted (ballot, value)
  }

  reaccept.replay { case (key, time, ballot) =>
    get (key, time) reaccepted (ballot)
  }

  close.replay { case (key, time, chosen, gen) =>
    get (key, time) closed (chosen, gen)
  }

  checkpoint.replay { meta =>
    archive.checkpoint (meta)
  }

  def launch (implicit launch: Disks.Launch): Async [Paxos] =
    supply {
      import launch.disks
      val kit = new PaxosKit (archive.close())
      medics.values foreach (_.close (kit))
      kit.acceptors.attach()
      kit.proposers.attach()
      kit
    }}
