package com.treode.store

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Disks
import com.treode.store.atomic.AtomicKit

private class RecoveryKit (implicit
    random: Random,
    scheduler: Scheduler,
    cluster: Cluster,
    recovery: Disks.Recovery,
    config: StoreConfig
) extends Store.Recovery {

  implicit val _catalogs = Catalogs.recover()
  val _paxos = Paxos.recover()
  val _atomic = AtomicKit.recover()

  val atlas = Atlas.recover()

  def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any): Unit =
    desc.listen (f)

  def launch (launch: Disks.Launch): Async [Store] = {
    import launch.disks

    for {
      catalogs <- _catalogs.launch (launch, atlas)
      paxos <- _paxos.launch (launch, atlas)
      atomic <- _atomic.launch (launch, atlas, paxos)
    } yield {
      atomic
    }}}
