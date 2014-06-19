package com.treode.store.atomic

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Disk
import com.treode.store.{Atlas, Library, Store}
import com.treode.store.paxos.Paxos

private [store] trait Atomic extends Store {

  def rebalance (atlas: Atlas): Async [Unit]
}

private [store] object Atomic {

  trait Recovery {

    def launch (implicit launch: Disk.Launch, cluster: Cluster, paxos: Paxos): Async [Atomic]
  }

  def recover() (implicit
      random: Random,
      scheduler: Scheduler,
      library: Library,
      recovery: Disk.Recovery,
      config: Store.Config
  ): Recovery =
    new RecoveryKit
}
