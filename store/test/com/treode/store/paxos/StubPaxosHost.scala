package com.treode.store.paxos

import com.treode.async.Async
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.implicits._
import com.treode.cluster.{Cluster, HostId}
import com.treode.cluster.stubs.StubCluster
import com.treode.disk.stubs.{StubDisks, StubDiskDrive}
import com.treode.store.{Atlas, Cohort, Library, StoreTestKit, StubStoreHost}
import com.treode.store.catalog.Catalogs

import Async.guard
import PaxosTestTools._

private class StubPaxosHost (val localId: HostId) (implicit kit: StoreTestKit)
extends StubStoreHost {
  import kit._

  implicit val cluster = new StubCluster (localId)

  implicit val library = new Library

  implicit val storeConfig = TestStoreConfig()
  implicit val recovery = StubDisks.recover()
  implicit val _catalogs = Catalogs.recover()
  val _paxos = Paxos.recover()

  val diskDrive = new StubDiskDrive

  val _launch =
    for {
      launch <- recovery.attach (diskDrive)
      catalogs <- _catalogs.launch (launch)
      paxos <- _paxos.launch (launch) .map (_.asInstanceOf [PaxosKit])
    } yield {
      launch.launch()
      (launch.disks, catalogs, paxos)
    }

  val captor = _launch.capture()
  scheduler.runTasks()
  while (!captor.wasInvoked)
    Thread.sleep (10)
  implicit val (disks, catalogs, paxos) = captor.passed

  catalogs.listen (Atlas.catalog) { atlas =>
    library.atlas = atlas
    library.residents = atlas.residents (localId)
  }

  cluster.startup()

  def setAtlas (cohorts: Cohort*) {
    val _cohorts = Atlas (cohorts.toArray, 1)
    library.atlas = _cohorts
    library.residents = _cohorts.residents (localId)
  }

  def issueAtlas (cohorts: Cohort*): Async [Unit] =
    catalogs.issue (Atlas.catalog) (1, Atlas (cohorts.toArray, 1))
}
