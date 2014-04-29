package com.treode.store

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Disks
import com.treode.store.atomic.AtomicKit
import com.treode.store.catalog.Catalogs
import com.treode.store.paxos.Paxos

import Store.Controller

private class RecoveryKit (implicit
    random: Random,
    scheduler: Scheduler,
    cluster: Cluster,
    recovery: Disks.Recovery,
    config: StoreConfig
) extends Store.Recovery {

  implicit val library = new Library

  implicit val _catalogs = Catalogs.recover()
  val _paxos = Paxos.recover()
  val _atomic = AtomicKit.recover()

  def launch (launch: Disks.Launch): Async [Controller] = {
    import launch.disks

    for {
      catalogs <- _catalogs.launch (launch)
      paxos <- _paxos.launch (launch)
      atomic <- _atomic.launch (launch, paxos)
    } yield {
      new ControllerAgent (catalogs, atomic)
    }}}
