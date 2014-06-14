package com.treode.store

import java.net.SocketAddress
import java.nio.file.Path
import java.util.concurrent.ExecutorService

import com.treode.async.Async
import com.treode.async.implicits._
import com.treode.cluster.{CellId, Cluster, HostId}
import com.treode.disk.{Disk, DriveAttachment, DriveDigest, DriveGeometry}

import Async.guard

private class ExtendedController (
    executor: ExecutorService,
    disk: Disk.Controller,
    cluster: Cluster,
    controller: Store.Controller
) extends Store.Controller {

  implicit val store: Store = controller.store

  def cohorts: Seq [Cohort] =
    controller.cohorts

  def cohorts_= (v: Seq [Cohort]): Unit =
    controller.cohorts = v

  def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any): Unit =
    controller.listen (desc) (f)

  def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Async [Unit] =
    controller.issue (desc) (version, cat)

  def drives: Async [Seq [DriveDigest]] =
    disk.drives

  def attach (items: DriveAttachment*): Async [Unit] =
    disk.attach (items:_*)

  def drain (paths: Path*): Async [Unit] =
    disk.drain (paths: _*)

  def cellId: CellId =
    cluster.cellId

  def hostId: HostId =
    cluster.localId

  def hail (remoteId: HostId, remoteAddr: SocketAddress): Unit =
    cluster.hail (remoteId, remoteAddr)

  def shutdown(): Async [Unit] =
    guard [Unit] {
      for {
        _ <- cluster.shutdown()
        _ <- disk.shutdown()
      } yield ()
    } .ensure {
      executor.shutdownNow()
    }}
