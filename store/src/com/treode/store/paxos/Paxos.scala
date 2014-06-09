package com.treode.store.paxos

import scala.util.Random

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Disk
import com.treode.store.{Atlas, Bytes, Library, StoreConfig, TxClock}

private [store] trait Paxos {

  def lead (key: Bytes, time: TxClock, value: Bytes): Async [Bytes]

  def propose (key: Bytes, time: TxClock, value: Bytes): Async [Bytes]

  def rebalance (atlas: Atlas): Async [Unit]
}

private [store] object Paxos {

  trait Recovery {

    def launch (implicit launch: Disk.Launch, cluster: Cluster): Async [Paxos]
  }

  def recover() (implicit
      random: Random,
      scheduler: Scheduler,
      library: Library,
      recovery: Disk.Recovery,
      config: StoreConfig
  ): Recovery =
    new RecoveryKit
}
