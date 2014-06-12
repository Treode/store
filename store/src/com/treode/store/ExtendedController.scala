package com.treode.store

import java.net.SocketAddress
import java.nio.file.Path
import java.util.concurrent.ExecutorService

import com.treode.async.Async
import com.treode.async.implicits._
import com.treode.cluster.{Cluster, HostId}
import com.treode.disk.{Disk, DiskGeometry}

import Async.guard

private class ExtendedController (
    executor: ExecutorService,
    disk: Disk.Controller,
    cluster: Cluster,
    controller: Store.Controller
) extends Store.Controller {

  implicit val store: Store = controller.store

  def attach (items: (Path, DiskGeometry)*): Async [Unit] =
    disk.attach (items:_*)

  def hail (remoteId: HostId, remoteAddr: SocketAddress): Unit =
    cluster.hail (remoteId, remoteAddr)

  def cohorts: Seq [Cohort] =
    controller.cohorts

  def cohorts_= (v: Seq [Cohort]): Unit =
    controller.cohorts = v

  def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any): Unit =
    controller.listen (desc) (f)

  def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Async [Unit] =
    controller.issue (desc) (version, cat)

  def shutdown(): Async [Unit] =
    guard [Unit] {
      for {
        _ <- cluster.shutdown()
        _ <- disk.shutdown()
      } yield ()
    } .ensure {
      executor.shutdownNow()
    }}
