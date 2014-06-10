package com.treode.store

import java.net.SocketAddress
import java.nio.file.Path
import java.util.concurrent.Executors
import scala.util.{Failure, Random}

import com.treode.async.{Async, AsyncIterator, Scheduler}
import com.treode.cluster.{CellId, Cluster, HostId, Peer}
import com.treode.disk.{Disk, DiskConfig, DiskGeometry}

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

    def attach (items: (Path, DiskGeometry)*): Async [Unit]

    def hail (remoteId: HostId, remoteAddr: SocketAddress)

    def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any)

    def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Async [Unit]
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
        cluster = Cluster.live (cellId, hostId, bindAddr, shareAddr) (random, scheduler)
        store <- _store.launch (launch, cluster)
      } yield {
        (new ExtendedController (executor, store)): Controller
      }
    } .rescue {
      case t: Throwable =>
        executor.shutdown()
        Failure (t)
    }}}
