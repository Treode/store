package com.treode.store

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.Disks
import com.treode.store.catalog.CatalogKit

private trait Catalogs {

  def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any)

  def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Async [Unit]
}

private object Catalogs {

  trait Recovery {

    def launch (implicit disks: Disks.Launch): Async [Catalogs]
  }

  def recover () (implicit
      random: Random,
      scheduler: Scheduler,
      cluster: Cluster,
      library: Library,
      recovery: Disks.Recovery,
      config: StoreConfig
  ): Recovery =
    CatalogKit.recover()
}
