package com.treode.store

import java.net.SocketAddress
import java.nio.file.Path
import java.util.concurrent.ExecutorService

import com.treode.async.Async
import com.treode.cluster.{Cluster, HostId}
import com.treode.disk.{Disk, DiskGeometry}
import com.treode.store.catalog.Catalogs

import Async.supply

private class SimpleController (
    cluster: Cluster,
    disk: Disk.Controller,
    library: Library,
    librarian: Librarian,
    catalogs: Catalogs,
    val store: Store
) extends Store.Controller {

  def cohorts: Seq [Cohort] =
    library.atlas.cohorts.toSeq

  def cohorts_= (v: Seq [Cohort]): Unit =
    librarian.issueAtlas (v.toArray)

  def attach (items: (Path, DiskGeometry)*): Async [Unit] =
    disk.attach (items: _*)

  def hail (remoteId: HostId, remoteAddr: SocketAddress): Unit =
    cluster.hail (remoteId, remoteAddr)

  def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any): Unit =
    catalogs.listen (desc) (f)

  def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Async [Unit] =
    catalogs.issue (desc) (version, cat)

  def shutdown(): Async [Unit] =
    supply()
}
