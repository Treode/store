package com.treode.store

import java.net.SocketAddress
import java.nio.file.Path
import java.util.concurrent.Executors
import scala.util.{Failure, Random}

import com.treode.async.{Async, AsyncIterator, Scheduler}
import com.treode.cluster.{CellId, Cluster, ClusterConfig, HostId, Peer}
import com.treode.disk.{Disk, DiskConfig, DiskGeometry, DriveAttachment, DriveDigest}

import Async.guard

trait Store {

  def read (rt: TxClock, ops: ReadOp*): Async [Seq [Value]]

  def write (xid: TxId, ct: TxClock, ops: WriteOp*): Async [TxClock]

  def status (xid: TxId): Async [TxStatus]

  def scan (table: TableId, start: Bound [Key], window: Window, slice: Slice): AsyncIterator [Cell]

  def hosts (slice: Slice): Seq [(Peer, Int)]
}

object Store {

  trait Controller {

    implicit def store: Store

    def cohorts: Seq [Cohort]

    def cohorts_= (cohorts: Seq [Cohort])

    def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any)

    def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Async [Unit]

    def drives: Async [Seq [DriveDigest]]

    def attach (items: DriveAttachment*): Async [Unit]

    def drain (paths: Path*): Async [Unit]

    def hail (remoteId: HostId, remoteAddr: SocketAddress)

    def shutdown(): Async [Unit]
  }

  trait Recovery {

    def launch (implicit launch: Disk.Launch, cluster: Cluster): Async [Controller]
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

  def recover() (implicit
      random: Random,
      scheduler: Scheduler,
      recovery: Disk.Recovery,
      config: StoreConfig
  ): Recovery =
    new RecoveryKit

  def recover (
      bindAddr: SocketAddress,
      shareAddr: SocketAddress,
      paths: Path*
  ) (implicit
      diskConfig: DiskConfig,
      clusterConfig: ClusterConfig,
      storeConfig: StoreConfig
  ): Async [Controller] = {
    val nthreads = Runtime.getRuntime.availableProcessors
    val executor = Executors.newScheduledThreadPool (nthreads)
    guard {
      implicit val random = Random
      implicit val scheduler = Scheduler (executor)
      implicit val _disk = Disk.recover()
      val _store = Store.recover()
      for {
        launch <- _disk.reattach (paths: _*)
        (hostId, cellId) = StorePicklers.sysid.fromByteArray (launch.sysid)
        cluster = Cluster.live (cellId, hostId, bindAddr, shareAddr)
        store <- _store.launch (launch, cluster)
      } yield {
        launch.launch()
        cluster.startup()
        (new ExtendedController (executor, launch.controller, cluster, store)): Controller
      }
    } .rescue {
      case t: Throwable =>
        executor.shutdown()
        Failure (t)
    }}}
