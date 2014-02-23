package com.treode.store.paxos

import scala.util.Random

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Disks
import com.treode.store.{Bytes, StoreConfig}

trait Paxos {

  def lead (key: Bytes, value: Bytes): Async [Bytes]
  def propose (key: Bytes, value: Bytes): Async [Bytes]
}

object Paxos {

  trait Recovery {
    def launch (implicit launch: Disks.Launch): Async [Paxos]
  }

  def recover() (implicit
      random: Random,
      scheduler: Scheduler,
      cluster: Cluster,
      recover: Disks.Recovery,
      config: StoreConfig
  ): Recovery =
    Acceptors.attach (new RecoveryKit)
}
