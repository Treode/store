package com.treode.store.catalog

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Disk
import com.treode.store.{CatalogDescriptor, Library, Store}

private [store] trait Catalogs {

  def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any)

  def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Async [Unit]
}

private [store] object Catalogs {

  trait Recovery {

    def launch (implicit disk: Disk.Launch, cluster: Cluster): Async [Catalogs]
  }

  def recover () (implicit
      random: Random,
      scheduler: Scheduler,
      library: Library,
      recovery: Disk.Recovery,
      config: Store.Config
  ): Recovery =
    new RecoveryKit
}
