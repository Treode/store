package com.treode.store.paxos

import scala.util.Random

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Disks
import com.treode.store.{Bytes, Library, StoreConfig, TxClock}

private [store] trait Paxos {

  def lead (key: Bytes, time: TxClock, value: Bytes): Async [Bytes]

  def propose (key: Bytes, time: TxClock, value: Bytes): Async [Bytes]
}

private [store] object Paxos {

  trait Recovery {

    def launch (implicit launch: Disks.Launch): Async [Paxos]
  }

  def recover() (implicit
      random: Random,
      scheduler: Scheduler,
      cluster: Cluster,
      library: Library,
      recovery: Disks.Recovery,
      config: StoreConfig
  ): Recovery =
    new RecoveryKit
}
