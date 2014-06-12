package com.treode.store.atomic

import scala.util.Random

import com.treode.async.{Async, AsyncIterator, Scheduler}
import com.treode.async.implicits._
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.cluster.{Cluster, HostId, Peer}
import com.treode.cluster.stubs.{StubPeer, StubNetwork}
import com.treode.disk.Disk
import com.treode.disk.stubs.{StubDisk, StubDiskDrive}
import com.treode.store._
import com.treode.store.catalog.Catalogs
import com.treode.store.paxos.Paxos
import org.scalatest.Assertions

import Async.latch
import Assertions._
import AtomicTestTools._

private class StubAtomicHost (
    val localId: HostId
) (implicit
    val random: Random,
    val scheduler: ChildScheduler,
    val cluster: StubPeer,
    val disk: Disk,
    val library: Library,
    val catalogs: Catalogs,
    val paxos: Paxos,
    val atomic: AtomicKit
) extends StoreClusterChecks.Host {

  val librarian = Librarian { atlas =>
    latch (paxos.rebalance (atlas), atomic.rebalance (atlas)) .map (_ => ())
  }

  cluster.startup()

  def shutdown(): Async [Unit] =
    for {
      _ <- cluster.shutdown()
    } yield {
      scheduler.shutdown()
    }

  def setAtlas (cohorts: Cohort*) {
    val atlas = Atlas (cohorts.toArray, 1)
    library.atlas = atlas
    library.residents = atlas.residents (localId)
  }

  def issueAtlas (cohorts: Cohort*): Unit =
    librarian.issueAtlas (cohorts.toArray)

  def atlas: Atlas =
    library.atlas

  def unsettled: Boolean =
    !library.atlas.settled

  def deputiesOpen: Boolean =
    !atomic.writers.deputies.isEmpty

  def audit: AsyncIterator [(TableId, Cell)] =
    for {
      e <- atomic.tables.tables.entrySet.async
      cell <- e.getValue.iterator (Residents.all)
    } yield {
      (e.getKey, cell)
    }

  def read (rt: TxClock, ops: ReadOp*): Async [Seq [Value]] =
    atomic.read (rt, ops:_*)

  def write (xid: TxId, ct: TxClock, ops: WriteOp*): Async [TxClock] =
    atomic.write (xid, ct, ops:_*)

  def status (xid: TxId): Async [TxStatus] =
    atomic.status (xid)

  def scan (table: TableId, start: Bound [Key], window: Window, slice: Slice): CellIterator =
    atomic.scan (table, start, window, slice)

  def hosts (slice: Slice): Seq [(Peer, Int)] =
    atomic.hosts (slice)

  def putCells (id: TableId, cs: Cell*) (implicit scheduler: StubScheduler): Unit =
    atomic.tables.receive (id, cs) .pass
}

private object StubAtomicHost extends StoreClusterChecks.Package [StubAtomicHost] {

  def boot (
      id: HostId,
      drive: StubDiskDrive,
      init: Boolean
  ) (implicit
      random: Random,
      parent: Scheduler,
      network: StubNetwork,
      config: StoreTestConfig
  ): Async [StubAtomicHost] = {

    import config._

    implicit val scheduler = new ChildScheduler (parent)
    implicit val cluster = new StubPeer (id)
    implicit val library = new Library
    implicit val recovery = StubDisk.recover()
    implicit val _catalogs = Catalogs.recover()
    val _paxos = Paxos.recover()
    val _atomic = Atomic.recover()

    for {
      launch <- if (init) recovery.attach (drive) else recovery.reattach (drive)
      catalogs <- _catalogs.launch (launch, cluster)
      paxos <- _paxos.launch (launch, cluster)
      atomic <- _atomic.launch (launch, cluster, paxos) .map (_.asInstanceOf [AtomicKit])
    } yield {
      launch.launch()
      new StubAtomicHost (id) (random, scheduler, cluster, launch.disk, library, catalogs, paxos, atomic)
    }}

  def install () (implicit kit: StoreTestKit): Async [StubAtomicHost] = {
    import kit._
    implicit val config = StoreTestConfig()
    boot (random.nextLong, new StubDiskDrive, true)
  }}
