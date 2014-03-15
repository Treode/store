package com.treode.store

import java.net.SocketAddress
import java.nio.file.Path
import java.util.concurrent.{ExecutorService, Executors}
import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.{Cluster, HostId}
import com.treode.disk.{Disks, DisksConfig, DiskGeometry}

object StandAlone {

  class Controller (
      executor: ExecutorService,
      cluster: Cluster,
      disks: Disks
  ) (implicit
      val store: Store
  ) {

    def attach (items: Seq [(Path, DiskGeometry)]): Async [Unit] =
      disks.attach (items, executor)

    def startup(): Unit =
      cluster.startup()
  }

  def create (
      localId: HostId,
      localAddr: SocketAddress,
      disksConfig: DisksConfig,
      storeConfig: StoreConfig,
      items: Seq [(Path, DiskGeometry)]
  ): Async [Controller] = {

    val random = Random

    val nthreads = Runtime.getRuntime.availableProcessors
    val executor = Executors.newScheduledThreadPool (nthreads)
    val scheduler = Scheduler (executor)

    val cluster = Cluster.live (localId, localAddr) (random, scheduler)

    val _disks = Disks.recover () (scheduler, disksConfig)

    val _store = Store.recover () (random, scheduler, cluster, _disks, storeConfig)

    for {
      launch <- _disks.attach (items, executor)
      store <- _store.launch (Cohort.settled (localId)) (launch)
    } yield {
      new Controller (executor, cluster, launch.disks) (store)
    }}

  def recover (
      localId: HostId,
      localAddr: SocketAddress,
      disksConfig: DisksConfig,
      storeConfig: StoreConfig,
      items: Seq [Path]
  ): Async [Controller] = {

    val random = Random

    val nthreads = Runtime.getRuntime.availableProcessors
    val executor = Executors.newScheduledThreadPool (nthreads)
    val scheduler = Scheduler (executor)

    val cluster = Cluster.live (localId, localAddr) (random, scheduler)

    val _disks = Disks.recover () (scheduler, disksConfig)

    val _store = Store.recover () (random, scheduler, cluster, _disks, storeConfig)

    for {
      launch <- _disks.reattach (items, executor)
      store <- _store.launch (Cohort.settled (localId)) (launch)
    } yield {
      new Controller (executor, cluster, launch.disks) (store)
    }}}
