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
import org.scalatest.Assertions

import Assertions._
import Async.{guard, supply}
import PaxosTestTools._

private class StubPaxosHost (
    val localId: HostId
) (implicit
    val random: Random,
    val scheduler: Scheduler,
    val cluster: Cluster,
    val disks: Disks,
    val library: Library,
    val catalogs: Catalogs,
    val paxos: PaxosKit
) extends StubStoreHost {

  val librarian = Librarian (paxos.rebalance _)

  cluster.startup()

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
  }}

private object StubPaxosHost {

  private def boot (
      id: HostId,
      drive: StubDiskDrive,
      init: Boolean
  ) (implicit
      kit: StoreTestKit
  ): Async [StubPaxosHost] = {
    import kit.{random, scheduler, network}

    implicit val cluster = new StubCluster (id)
    implicit val library = new Library
    implicit val storeConfig = TestStoreConfig()
    implicit val recovery = StubDisks.recover()
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

  def install () (implicit kit: StoreTestKit): Async [StubPaxosHost] = {
    import kit.random
    boot (random.nextLong, new StubDiskDrive, true)
  }}
