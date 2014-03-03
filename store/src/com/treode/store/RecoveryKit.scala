package com.treode.store

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Disks
import com.treode.store.atomic.AtomicKit
import com.treode.store.catalog.CohortCatalog

private class RecoveryKit (implicit
    random: Random,
    scheduler: Scheduler,
    cluster: Cluster,
    recovery: Disks.Recovery,
    config: StoreConfig
) extends Store.Recovery {

  val _catalogs = Catalogs.recover()
  val _cohorts = CohortCatalog.recover (_catalogs)
  val _paxos = Paxos.recover()
  val _atomic = AtomicKit.recover()

  def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any): Unit =
    _catalogs.listen (desc) (f)

  def launch (implicit launch: Disks.Launch): Async [Store] = {
    import launch.disks

    for {
      catalogs <- _catalogs.launch (launch)
      cohorts <- _cohorts.launch()
      paxos <- _paxos.launch (launch, cohorts)
      atomic <- _atomic.launch (launch, cohorts, paxos)
    } yield {
      atomic
    }}}
