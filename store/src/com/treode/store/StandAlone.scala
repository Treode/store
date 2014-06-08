package com.treode.store

import java.net.SocketAddress
import java.nio.file.Path
import java.util.concurrent.{ExecutorService, Executors}
import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.{Cluster, HostId}
import com.treode.disk.{Disk, DiskConfig, DiskGeometry}

object StandAlone {

  class Controller (
      executor: ExecutorService,
      cluster: Cluster,
      disks: Disk.Controller,
      controller: Store.Controller
  ) extends Store.Controller {

    implicit val store: Store = controller.store

    def attach (items: (Path, DiskGeometry)*): Async [Unit] =
      disks.attach (items:_*)

    def hail (remoteId: HostId, remoteAddr: SocketAddress): Unit =
      cluster.hail (remoteId, remoteAddr)

    def startup(): Unit =
      cluster.startup()

    def cohorts: Seq [Cohort] =
      controller.cohorts

    def cohorts_= (v: Seq [Cohort]): Unit =
      controller.cohorts = v

    def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any): Unit =
      controller.listen (desc) (f)

    def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Async [Unit] =
      controller.issue (desc) (version, cat)
  }

  def create (
      localId: HostId,
      localAddr: SocketAddress,
      disksConfig: DiskConfig,
      storeConfig: StoreConfig,
      items: (Path, DiskGeometry)*
  ): Async [Controller] = {

    val random = Random

    val nthreads = Runtime.getRuntime.availableProcessors
    val executor = Executors.newScheduledThreadPool (nthreads)
    val scheduler = Scheduler (executor)

    val cluster = Cluster.live (localId, localAddr) (random, scheduler)

    val _disks = Disk.recover () (scheduler, disksConfig)

    val _store = Store.recover () (random, scheduler, cluster, _disks, storeConfig)

    for {
      launch <- _disks.attach (items: _*)
      store <- _store.launch (launch)
    } yield {
      new Controller (executor, cluster, launch.controller, store)
    }}

  def recover (
      localId: HostId,
      localAddr: SocketAddress,
      disksConfig: DiskConfig,
      storeConfig: StoreConfig,
      items: Path*
  ): Async [Controller] = {

    val random = Random

    val nthreads = Runtime.getRuntime.availableProcessors
    val executor = Executors.newScheduledThreadPool (nthreads)
    val scheduler = Scheduler (executor)

    val cluster = Cluster.live (localId, localAddr) (random, scheduler)

    val _disks = Disk.recover () (scheduler, disksConfig)

    val _store = Store.recover () (random, scheduler, cluster, _disks, storeConfig)

    for {
      launch <- _disks.reattach (items: _*)
      store <- _store.launch (launch)
    } yield {
      new Controller (executor, cluster, launch.controller, store)
    }}}
