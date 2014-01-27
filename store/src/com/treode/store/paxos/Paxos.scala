package com.treode.store.paxos

import scala.util.Random

import com.treode.async.{Callback, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Disks
import com.treode.store.{Bytes, StoreConfig}

trait Paxos {

  def lead (key: Bytes, value: Bytes, cb: Callback [Bytes])
  def propose (key: Bytes, value: Bytes, cb: Callback [Bytes])
}

object Paxos {

  def apply () (implicit random: Random, scheduler: Scheduler, cluster: Cluster, disks: Disks,
      config: StoreConfig): Paxos = {

    val kit = new PaxosKit
    Acceptors.attach (kit, Callback.ignore)
    kit
  }}
