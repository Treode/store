package com.treode.store.paxos

import scala.util.Random

import com.treode.async.{Callback, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Recovery
import com.treode.store.{Bytes, StoreConfig}

trait Paxos {

  def lead (key: Bytes, value: Bytes, cb: Callback [Bytes])
  def propose (key: Bytes, value: Bytes, cb: Callback [Bytes])
}

object Paxos {

  def recover (cb: Callback [Paxos]) (implicit random: Random, scheduler: Scheduler, cluster: Cluster,
      recover: Recovery, config: StoreConfig) {

    val kit = new PaxosRecovery
    Acceptors.attach (kit, cb)
  }}
