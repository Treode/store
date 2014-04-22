package com.treode.store.paxos

import java.nio.file.Paths
import com.treode.async.Async
import com.treode.async.io.StubFile
import com.treode.cluster.{Cluster, HostId, StubActiveHost, StubHost, StubNetwork}
import com.treode.disk.{Disks, DisksConfig, DiskGeometry}
import com.treode.store.{Catalogs, Cohort, Cohorts, Library, Paxos}

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
  val files = Seq ((Paths.get ("a"), file, geometry))

  val _launch =
    for {
      launch <- recovery.attach (files)
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

  Cohorts.catalog.listen { atlas =>
    library.atlas = atlas
    library.residents = atlas.residents (localId)
  }

  def setCohorts (cohorts: Cohort*) {
    val _cohorts = Cohorts (cohorts.toArray, 1)
    library.atlas = _cohorts
    library.residents = _cohorts.residents (localId)
  }

  def issueCohorts (cohorts: Cohort*): Async [Unit] =
    Cohorts.catalog.issue (1, Cohorts (cohorts.toArray, 1))
}
