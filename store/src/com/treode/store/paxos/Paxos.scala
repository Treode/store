package com.treode.store.paxos

import scala.util.Random

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Recovery
import com.treode.store.{Bytes, StoreConfig}

trait Paxos {

  def lead (key: Bytes, value: Bytes): Async [Bytes]
  def propose (key: Bytes, value: Bytes): Async [Bytes]
}

object Paxos {

  def recover() (implicit random: Random, scheduler: Scheduler, cluster: Cluster,
      recover: Recovery, config: StoreConfig): Async [Paxos] =
    Acceptors.attach (new PaxosRecovery)
}
