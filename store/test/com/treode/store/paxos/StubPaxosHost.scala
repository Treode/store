package com.treode.store.paxos

import java.nio.file.Paths

import com.treode.async.Async
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.implicits._
import com.treode.cluster.{Cluster, HostId}
import com.treode.cluster.stubs.{StubActiveHost, StubHost, StubNetwork}
import com.treode.disk.{Disks, DisksConfig, DiskGeometry}
import com.treode.store.{Atlas, Cohort, Library}
import com.treode.store.catalog.Catalogs

import Async.guard
import PaxosTestTools._

private class StubPaxosHost (id: HostId, network: StubNetwork)
extends StubActiveHost (id, network) {
  import network.{random, scheduler}

  implicit val cluster: Cluster = this
  implicit val library = new Library

  implicit val disksConfig = TestDisksConfig()
  implicit val storeConfig = TestStoreConfig()

  implicit val recovery = Disks.recover()
  implicit val _catalogs = Catalogs.recover()
  val _paxos = Paxos.recover()

  val file = new StubFile
  val geometry = TestDiskGeometry()

  val _launch =
    for {
      launch <- recovery._attach (("a", file, geometry))
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

  def setAtlas (cohorts: Cohort*) {
    val _cohorts = Atlas (cohorts.toArray, 1)
    library.atlas = _cohorts
    library.residents = _cohorts.residents (localId)
  }

  def issueAtlas (cohorts: Cohort*): Async [Unit] =
    catalogs.issue (Atlas.catalog) (1, Atlas (cohorts.toArray, 1))
}
