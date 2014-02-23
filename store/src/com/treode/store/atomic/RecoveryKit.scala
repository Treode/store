package com.treode.store.atomic

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Disks
import com.treode.store.{LocalStore, StoreConfig}
import com.treode.store.paxos.Paxos

import Async.supply

private class RecoveryKit (implicit
    val random: Random,
    val scheduler: Scheduler,
    val cluster: Cluster,
    val recovery: Disks.Recovery,
    val store: LocalStore,
    val config: StoreConfig
) extends AtomicKit.Recovery {

  def launch (implicit launch: Disks.Launch, paxos: Paxos): Async [AtomicKit] =
    supply (new AtomicKit (0))
}
