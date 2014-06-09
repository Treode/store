package com.treode.store

import java.net.SocketAddress
import java.nio.file.Path
import java.util.concurrent.{ExecutorService, Executors}
import scala.util.{Failure, Random}

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.{CellId, Cluster, HostId}
import com.treode.disk.{Disk, DiskConfig, DiskGeometry}

import Async.guard

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

  def init (
      hostId: HostId,
      cellId: CellId,
      superBlockBits: Int,
      segmentBits: Int,
      blockBits: Int,
      diskBytes: Long,
      paths: Path*
  ): Unit = {
    val sysid = StorePicklers.sysid.toByteArray ((hostId, cellId))
    Disk.init (sysid, superBlockBits, segmentBits, blockBits, diskBytes, paths: _*)
  }

  def recover (
      localAddr: SocketAddress,
      disksConfig: DiskConfig,
      storeConfig: StoreConfig,
      paths: Path*
  ): Async [Controller] = {
    val nthreads = Runtime.getRuntime.availableProcessors
    val executor = Executors.newScheduledThreadPool (nthreads)
    guard {
      val random = Random
      val scheduler = Scheduler (executor)
      val _disks = Disk.recover () (scheduler, disksConfig)
      val _store = Store.recover () (random, scheduler, _disks, storeConfig)
      for {
        launch <- _disks.reattach (paths: _*)
        (hostId, cellId) = StorePicklers.sysid.fromByteArray (launch.sysid)
        cluster = Cluster.live (cellId, hostId, localAddr) (random, scheduler)
        store <- _store.launch (launch, cluster)
      } yield {
        new Controller (executor, cluster, launch.controller, store)
      }
    } .rescue {
      case t: Throwable =>
        executor.shutdown()
        Failure (t)
    }}}
