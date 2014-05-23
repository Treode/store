package com.treode.store.paxos

import scala.collection.JavaConversions
import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.implicits._
import com.treode.async.misc.materialize
import com.treode.cluster.Cluster
import com.treode.disk.Disk
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
    val recovery: Disk.Recovery,
    val config: StoreConfig
) extends Paxos.Recovery {

  val archive = TierMedic (Acceptors.archive, 0)
  val medics = newMedicsMap

  def get (key: Bytes, time: TxClock, default: Option [Bytes]): Medic = {
    var m0 = medics.get ((key, time))
    if (m0 != null) return m0
    val m1 = Medic (key, time, default, this)
    m0 = medics.putIfAbsent ((key, time), m1)
    if (m0 != null) return m0
    return m1
  }

  open.replay { case (key, time, default) =>
    get (key, time, Some (default)) opened (default)
  }

  promise.replay { case (key, time, ballot) =>
    get (key, time, None) promised (ballot)
  }

  accept.replay { case (key, time, ballot, value) =>
    get (key, time, Some (value)) accepted (ballot, value)
  }

  reaccept.replay { case (key, time, ballot) =>
    get (key, time, None) reaccepted (ballot)
  }

  close.replay { case (key, time, chosen, gen) =>
    get (key, time, Some (chosen)) closed (chosen, gen)
  }

  checkpoint.replay { meta =>
    archive.checkpoint (meta)
  }

  def launch (implicit launch: Disk.Launch): Async [Paxos] =
    supply {
      import launch.disks
      val kit = new PaxosKit (archive.close())
      medics.values foreach (_.close (kit))
      kit.acceptors.attach()
      kit.proposers.attach()
      kit
    }}
