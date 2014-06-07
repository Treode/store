package com.treode.store.atomic

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Disk
import com.treode.store.{Atlas, Library, Store, StoreConfig}
import com.treode.store.paxos.Paxos

private [store] trait Atomic extends Store {

  def rebalance (atlas: Atlas): Async [Unit]
}

private [store] object Atomic {

  trait Recovery {

    def launch (implicit launch: Disk.Launch, paxos: Paxos): Async [Atomic]
  }

  def recover() (implicit
      random: Random,
      scheduler: Scheduler,
      cluster: Cluster,
      library: Library,
      recovery: Disk.Recovery,
      config: StoreConfig
  ): Recovery =
    new RecoveryKit
}
