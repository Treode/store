package com.treode.store

import java.net.SocketAddress
import java.nio.file.Path
import java.util.concurrent.Executors
import scala.util.{Failure, Random}

import com.treode.async.{Async, AsyncIterator, Backoff, Scheduler}
import com.treode.async.misc.RichInt
import com.treode.cluster.{CellId, Cluster, HostId, Peer, RumorDescriptor}
import com.treode.disk.{Disk, DriveAttachment, DriveDigest, DriveGeometry}

import Async.guard

trait Store {

  def read (rt: TxClock, ops: ReadOp*): Async [Seq [Value]]

  def write (xid: TxId, ct: TxClock, ops: WriteOp*): Async [TxClock]

  def status (xid: TxId): Async [TxStatus]

  def scan (table: TableId, start: Bound [Key], window: Window, slice: Slice): AsyncIterator [Cell]

  def hosts (slice: Slice): Seq [(HostId, Int)]
}

object Store {

  case class Config (
      closedLifetime: Int,
      deliberatingTimeout: Int,
      exodusThreshold: Double,
      falsePositiveProbability: Double,
      lockSpaceBits: Int,
      moveBatchBackoff: Backoff,
      moveBatchBytes: Int,
      moveBatchEntries: Int,
      prepareBackoff: Backoff,
      preparingTimeout: Int,
      priorValueEpoch: Epoch,
      proposingBackoff: Backoff,
      readBackoff: Backoff,
      scanBatchBackoff: Backoff,
      scanBatchBytes: Int,
      scanBatchEntries: Int,
      targetPageBytes: Int
  ) {

    require (
        closedLifetime > 0,
        "The closed lifetime must be more than 0 milliseconds.")

    require (
        deliberatingTimeout > 0,
        "The deliberating timeout must be more than 0 milliseconds.")

    require (
        0.0 < exodusThreshold && exodusThreshold < 1.0,
        "The exodus threshould must be between 0 and 1 exclusive.")

    require (
        0 < falsePositiveProbability && falsePositiveProbability < 1,
        "The false positive probability must be between 0 and 1 exclusive.")

    require (
        0 <= lockSpaceBits && lockSpaceBits <= 14,
        "The size of the lock space must be between 0 and 14 bits.")

    require (
        moveBatchBytes > 0,
        "The move batch size must be more than 0 bytes.")

    require (
        moveBatchEntries > 0,
        "The move batch size must be more than 0 entries.")

    require (
        preparingTimeout > 0,
        "The preparing timeout must be more than 0 milliseconds.")

    require (
        scanBatchBytes > 0,
        "The scan batch size must be more than 0 bytes.")

    require (
        scanBatchEntries > 0,
        "The scan batch size must be more than 0 entries.")

    require (
        targetPageBytes > 0,
        "The target size of a page must be more than zero bytes.")
  }

  object Config {

    val suggested = Config (
        closedLifetime = 2.seconds,
        deliberatingTimeout = 2.seconds,
        exodusThreshold = 0.2D,
        falsePositiveProbability = 0.01,
        lockSpaceBits = 10,
        moveBatchBackoff = Backoff (2.seconds, 1.seconds, 1.minutes, 7),
        moveBatchBytes = 1<<20,
        moveBatchEntries = 10000,
        prepareBackoff = Backoff (100, 100, 1.seconds, 7),
        preparingTimeout = 5.seconds,
        priorValueEpoch = Epoch.StartOfYesterday,
        proposingBackoff = Backoff (100, 100, 1.seconds, 7),
        readBackoff = Backoff (100, 100, 1.seconds, 7),
        scanBatchBytes = 1<<16,
        scanBatchEntries = 1000,
        scanBatchBackoff = Backoff (700, 300, 10.seconds, 7),
        targetPageBytes = 1<<20)
  }

  trait Controller {

    implicit def store: Store

    def cohorts: Seq [Cohort]

    def cohorts_= (cohorts: Seq [Cohort])

    def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any)

    def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Async [Unit]

    def drives: Async [Seq [DriveDigest]]

    def attach (items: DriveAttachment*): Async [Unit]

    def drain (paths: Path*): Async [Unit]

    def cellId: CellId

    def hostId: HostId

    def hail (remoteId: HostId, remoteAddr: SocketAddress)

    def listen [M] (desc: RumorDescriptor [M]) (f: (M, Peer) => Any)

    def spread [M] (desc: RumorDescriptor [M]) (msg: M)

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
      config: Store.Config
  ): Recovery =
    new RecoveryKit

  def recover (
      bindAddr: SocketAddress,
      shareAddr: SocketAddress,
      paths: Path*
  ) (implicit
      diskConfig: Disk.Config,
      clusterConfig: Cluster.Config,
      storeConfig: Store.Config
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
