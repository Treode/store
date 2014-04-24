package com.treode.store.atomic

import java.nio.file.Paths
import scala.util.Random

import com.treode.async.{Async, Callback}
import com.treode.async.io.StubFile
import com.treode.cluster.{Cluster, HostId, StubActiveHost, StubNetwork}
import com.treode.store._
import com.treode.disk.Disks
import org.scalatest.Assertions

import Assertions.assertResult
import AtomicTestTools._
import Callback.ignore

private class StubAtomicHost (id: HostId, network: StubNetwork)
extends StubActiveHost (id, network) {
  import network.{random, scheduler}

  implicit val cluster: Cluster = this
  implicit val library = new Library

  implicit val disksConfig = TestDisksConfig()
  implicit val storeConfig = TestStoreConfig()

  implicit val recovery = Disks.recover()
  implicit val _catalogs = Catalogs.recover()
  val _paxos = Paxos.recover()
  val _atomic = AtomicKit.recover()

  val file = new StubFile
  val geometry = TestDiskGeometry()
  val files = Seq ((Paths.get ("a"), file, geometry))

  val _launch =
    for {
      launch <- recovery.attach (files)
      catalogs <- _catalogs.launch (launch)
      paxos <- _paxos.launch (launch)
      atomic <- _atomic.launch (launch, paxos) .map (_.asInstanceOf [AtomicKit])
    } yield {
      launch.launch()
      (launch.disks, catalogs, atomic)
    }

  val captor = _launch.capture()
  scheduler.runTasks()
  while (!captor.wasInvoked)
    Thread.sleep (10)
  implicit val (disks, catalogs, atomic) = captor.passed

  val librarian = new Librarian (atomic.rebalance _)

  scuttlebutt.attach (this)

  def setAtlas (cohorts: Cohort*) {
    val version = library.atlas.version + 1
    val atlas = Atlas (cohorts.toArray, version)
    library.atlas = atlas
    library.residents = atlas.residents (localId)
  }

  def issueAtlas (cohorts: Cohort*) {
    val version = library.atlas.version + 1
    val atlas = Atlas (cohorts.toArray, version)
    library.atlas = atlas
    library.residents = atlas.residents (localId)
    Atlas.catalog.issue (version, atlas) .pass
  }

  def writer (xid: TxId) = atomic.writers.get (xid)

  def read (rt: TxClock, ops: Seq [ReadOp]): Async [Seq [Value]] =
    atomic.read (rt, ops)

  def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp]): Async [TxClock] =
    atomic.write (xid, ct, ops)

  def expectAtlas (atlas: Atlas) {
    assertResult (atlas) (library.atlas)
    assertResult (librarian.issued) (atlas.version)
    assert (librarian.receipts forall (_._2 == atlas.version))
  }

  def expectCells (id: TableId) (cs: Cell*) {
    val t = atomic.tables.tables.get (id)
    assertResult (cs) (t .iterator (library.residents) .toSeq)
  }}
