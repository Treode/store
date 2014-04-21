package com.treode.store.paxos

import java.nio.file.Paths
import com.treode.async.Async
import com.treode.async.io.StubFile
import com.treode.cluster.{Cluster, HostId, StubActiveHost, StubHost, StubNetwork}
import com.treode.disk.{Disks, DisksConfig, DiskGeometry}
import com.treode.store.{Atlas, Catalogs, Cohort, Cohorts, Paxos}
import com.treode.store.atlas.AtlasKit

import Async.guard
import PaxosTestTools._

private class StubPaxosHost (id: HostId, network: StubNetwork)
extends StubActiveHost (id, network) {
  import network.{random, scheduler}

  implicit val cluster: Cluster = this

  implicit val disksConfig = TestDisksConfig()
  implicit val storeConfig = TestStoreConfig()

  implicit val recovery = Disks.recover()
  implicit val _catalogs = Catalogs.recover()
  val _paxos = Paxos.recover()

  val file = new StubFile
  val geometry = TestDiskGeometry()
  val files = Seq ((Paths.get ("a"), file, geometry))

  val atlas = Atlas.recover() .asInstanceOf [AtlasKit]

  val _launch =
    for {
      launch <- recovery.attach (files)
      catalogs <- _catalogs.launch (launch, atlas)
      paxos <- _paxos.launch (launch, atlas) .map (_.asInstanceOf [PaxosKit])
    } yield {
      launch.launch()
      (launch.disks, catalogs, paxos)
    }

  val captor = _launch.capture()
  scheduler.runTasks()
  while (!captor.wasInvoked)
    Thread.sleep (10)
  implicit val (disks, catalogs, paxos) = captor.passed

  def setCohorts (cohorts: Cohort*): Unit =
    atlas.set (Cohorts (cohorts.toArray, 1))

  def issueCohorts (cohorts: Cohort*): Async [Unit] =
    atlas.issue (Cohorts (cohorts.toArray, 1))
}
