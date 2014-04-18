package com.treode.store

import scala.util.Random

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Disks
import com.treode.store.paxos.PaxosKit

private trait Paxos {

  def lead (key: Bytes, time: TxClock, value: Bytes): Async [Bytes]

  def propose (key: Bytes, time: TxClock, value: Bytes): Async [Bytes]
}

private object Paxos {

  trait Recovery {

    def launch (implicit launch: Disks.Launch, atlas: Atlas): Async [Paxos]
  }

  def recover() (implicit
      random: Random,
      scheduler: Scheduler,
      cluster: Cluster,
      recovery: Disks.Recovery,
      config: StoreConfig
  ): Recovery =
   PaxosKit.recover()
}
