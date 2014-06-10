package com.treode.store

import java.util.concurrent.ExecutorService
import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Disk
import com.treode.store.atomic.Atomic
import com.treode.store.catalog.Catalogs
import com.treode.store.paxos.Paxos

import Async.latch
import Store.Controller

private class RecoveryKit (implicit
    random: Random,
    scheduler: Scheduler,
    recovery: Disk.Recovery,
    config: StoreConfig
) extends Store.Recovery {

  implicit val library = new Library

  implicit val _catalogs = Catalogs.recover()
  val _paxos = Paxos.recover()
  val _atomic = Atomic.recover()

  def launch (implicit launch: Disk.Launch, cluster: Cluster): Async [Controller] = {
    import launch.disks

    for {
      catalogs <- _catalogs.launch (launch, cluster)
      paxos <- _paxos.launch (launch, cluster)
      atomic <- _atomic.launch (launch, cluster, paxos)
    } yield {

      val librarian = Librarian { atlas =>
        latch (paxos.rebalance (atlas), atomic.rebalance (atlas)) .map (_ => ())
      } (scheduler, cluster, catalogs, library)

      new SimpleController (cluster, launch.controller, library, librarian, catalogs, atomic)
    }}}
