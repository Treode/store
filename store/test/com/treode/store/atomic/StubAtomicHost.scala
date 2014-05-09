package com.treode.store.atomic

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.cluster.{Cluster, HostId}
import com.treode.cluster.stubs.{StubCluster, StubNetwork}
import com.treode.disk.Disks
import com.treode.disk.stubs.{StubDisks, StubDiskDrive}
import com.treode.store._
import com.treode.store.catalog.Catalogs
import com.treode.store.paxos.Paxos
import org.scalatest.Assertions

import Assertions._
import AtomicTestTools._

private class StubAtomicHost (
    val localId: HostId
) (implicit
    val random: Random,
    val scheduler: Scheduler,
    val cluster: Cluster,
    val disks: Disks,
    val library: Library,
    val catalogs: Catalogs,
    val atomic: AtomicKit
) extends StubStoreHost {

  val librarian = new Librarian (atomic.rebalance _)

  cluster.startup()

  def setAtlas (cohorts: Cohort*) {
    val version = library.atlas.version + 1
    val atlas = Atlas (cohorts.toArray, version)
    library.atlas = atlas
    library.residents = atlas.residents (localId)
  }

  def issueAtlas (cohorts: Cohort*): Async [Unit] = {
    val version = library.atlas.version + 1
    val atlas = Atlas (cohorts.toArray, version)
    library.atlas = atlas
    library.residents = atlas.residents (localId)
    catalogs.issue (Atlas.catalog) (version, atlas)
  }

  def writer (xid: TxId) = atomic.writers.get (xid)

  def read (rt: TxClock, ops: ReadOp*): Async [Seq [Value]] =
    atomic.read (rt, ops:_*)

  def write (xid: TxId, ct: TxClock, ops: WriteOp*): Async [TxClock] =
    atomic.write (xid, ct, ops:_*)

  def scan (table: TableId, key: Bytes, time: TxClock): CellIterator =
    atomic.scan (table, key,  time)

  def putCells (id: TableId, cs: Cell*) (implicit scheduler: StubScheduler): Unit =
    atomic.tables.receive (id, cs) .pass

  def expectAtlas (atlas: Atlas) {
    assertResult (atlas) (library.atlas)
    assertResult (librarian.issued) (atlas.version)
    assert (librarian.receipts forall (_._2 == atlas.version))
  }

  def expectCells (id: TableId) (cs: Cell*) (implicit scheduler: StubScheduler) {
    val t = atomic.tables.tables.get (id)
    assertResult (cs) (t .iterator (library.residents) .toSeq)
  }}

private object StubAtomicHost {

  private def boot (
      id: HostId,
      drive: StubDiskDrive,
      init: Boolean
  ) (implicit
      kit: StoreTestKit
  ): Async [StubAtomicHost] = {
    import kit.{network, random, scheduler}

    implicit val cluster = new StubCluster (id)
    implicit val library = new Library
    implicit val storeConfig = TestStoreConfig()
    implicit val recovery = StubDisks.recover()
    implicit val _catalogs = Catalogs.recover()
    val _paxos = Paxos.recover()
    val _atomic = AtomicKit.recover()

    for {
      launch <- if (init) recovery.attach (drive) else recovery.reattach (drive)
      catalogs <- _catalogs.launch (launch)
      paxos <- _paxos.launch (launch)
      atomic <- _atomic.launch (launch, paxos) .map (_.asInstanceOf [AtomicKit])
    } yield {
      launch.launch()
      new StubAtomicHost (id) (random, scheduler, cluster, launch.disks, library, catalogs, atomic)
    }}

  def install () (implicit kit: StoreTestKit): Async [StubAtomicHost] =
    boot (kit.random.nextLong, new StubDiskDrive, true)
}
