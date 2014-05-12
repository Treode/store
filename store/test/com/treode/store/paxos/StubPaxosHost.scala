package com.treode.store.paxos

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.implicits._
import com.treode.cluster.{Cluster, HostId}
import com.treode.cluster.stubs.{StubCluster, StubNetwork}
import com.treode.disk.Disks
import com.treode.disk.stubs.{StubDisks, StubDiskDrive}
import com.treode.store._
import com.treode.store.catalog.Catalogs
import com.treode.store.tier.TierTable
import org.scalatest.Assertions

import Assertions._
import Async.{guard, supply}
import PaxosTestTools._

private class StubPaxosHost (
    val localId: HostId
) (implicit
    val random: Random,
    val scheduler: ChildScheduler,
    val cluster: StubCluster,
    val disks: Disks,
    val library: Library,
    val catalogs: Catalogs,
    val paxos: PaxosKit
) extends StoreClusterChecks.Host {

  val librarian = Librarian (paxos.rebalance _)

  cluster.startup()

  def shutdown() {
    cluster.shutdown()
    scheduler.shutdown()
  }

  def setAtlas (cohorts: Cohort*) {
    val _cohorts = Atlas (cohorts.toArray, 1)
    library.atlas = _cohorts
    library.residents = _cohorts.residents (localId)
  }

  def issueAtlas (cohorts: Cohort*): Async [Unit] = {
    val version = library.atlas.version + 1
    val atlas = Atlas (cohorts.toArray, version)
    library.atlas = atlas
    library.residents = atlas.residents (localId)
    catalogs.issue (Atlas.catalog) (version, atlas)
  }

  def expectAtlas (atlas: Atlas) {
    assertResult (atlas) (library.atlas)
    assertResult (librarian.issued) (atlas.version)
    assert (librarian.receipts forall (_._2 == atlas.version))
  }

  def archive: TierTable =
    paxos.archive

  def acceptors: AcceptorsMap =
    paxos.acceptors.acceptors

  def locate (key: Bytes, time: TxClock): Cohort =
    paxos.locate (key, time)

  def propose (key: Bytes, time: TxClock, value: Bytes): Async [Bytes] =
    paxos.propose (key, time, value)
}

private object StubPaxosHost extends StoreClusterChecks.Package [StubPaxosHost] {

  def boot (
      id: HostId,
      checkpoint: Double,
      compaction: Double,
      drive: StubDiskDrive,
      init: Boolean
  ) (implicit
      random: Random,
      parent: Scheduler,
      network: StubNetwork
  ): Async [StubPaxosHost] = {

    implicit val scheduler = new ChildScheduler (parent)
    implicit val cluster = new StubCluster (id)
    implicit val library = new Library
    implicit val storeConfig = TestStoreConfig()
    implicit val recovery = StubDisks.recover (checkpoint, compaction)
    implicit val _catalogs = Catalogs.recover()
    val _paxos = Paxos.recover()

    for {
      launch <- if (init) recovery.attach (drive) else recovery.reattach (drive)
      catalogs <- _catalogs.launch (launch)
      paxos <- _paxos.launch (launch) map (_.asInstanceOf [PaxosKit])
    } yield {
      launch.launch()
      new StubPaxosHost (id) (random, scheduler, cluster, launch.disks, library, catalogs, paxos)
    }}

  def install () (implicit r: Random, s: Scheduler, n: StubNetwork): Async [StubPaxosHost] =
    boot (r.nextLong, 0.1, 0.1, new StubDiskDrive, true)
}
