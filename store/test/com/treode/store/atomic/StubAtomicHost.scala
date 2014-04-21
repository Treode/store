package com.treode.store.atomic

import java.nio.file.Paths
import scala.util.Random

import com.treode.async.{Async, Callback, CallbackCaptor}
import com.treode.async.io.StubFile
import com.treode.cluster.{Cluster, HostId, StubActiveHost, StubHost, StubNetwork}
import com.treode.store._
import com.treode.store.atlas.AtlasKit
import com.treode.disk.{Disks, DisksConfig, DiskGeometry}
import org.scalatest.Assertions

import Assertions.assertResult
import AtomicTestTools._
import Callback.ignore

private class StubAtomicHost (id: HostId, network: StubNetwork)
extends StubActiveHost (id, network) {
  import network.{random, scheduler}

  implicit val cluster: Cluster = this

  implicit val disksConfig = TestDisksConfig()
  implicit val storeConfig = TestStoreConfig()

  implicit val recovery = Disks.recover()
  implicit val _catalogs = Catalogs.recover()
  val _paxos = Paxos.recover()
  val _atomic = AtomicKit.recover()

  val file = new StubFile
  val geometry = TestDiskGeometry()
  val files = Seq ((Paths.get ("a"), file, geometry))

  val atlas = Atlas.recover() .asInstanceOf [AtlasKit]

  val _launch =
    for {
      launch <- recovery.attach (files)
      catalogs <- _catalogs.launch (launch, atlas)
      paxos <- _paxos.launch (launch, atlas)
      atomic <- _atomic.launch (launch, atlas, paxos) .map (_.asInstanceOf [AtomicKit])
    } yield {
      launch.launch()
      (launch.disks, catalogs, atomic)
    }

  val captor = _launch.capture()
  scheduler.runTasks()
  while (!captor.wasInvoked)
    Thread.sleep (10)
  implicit val (disks, catalogs, atomic) = captor.passed

  def setCohorts (cohorts: Cohort*): Unit =
    atlas.set (Cohorts (cohorts.toArray, 1))

  def issueCohorts (cohorts: Cohort*): Async [Unit] =
    atlas.issue (Cohorts (cohorts.toArray, 1))

  def writer (xid: TxId) = atomic.writers.get (xid)

  def read (rt: TxClock, ops: Seq [ReadOp]): Async [Seq [Value]] =
    atomic.read (rt, ops)

  def write (xid: TxId, ct: TxClock, ops: Seq [WriteOp]): Async [WriteResult] =
    atomic.write (xid, ct, ops)

  def expectCells (id: TableId) (cs: Cell*) {
    val t = atomic.tables.tables.get (id)
    assertResult (cs) (t .iterator (Residents.all) .toSeq)
  }}
