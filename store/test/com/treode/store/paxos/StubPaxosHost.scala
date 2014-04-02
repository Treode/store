package com.treode.store.paxos

import java.nio.file.Paths
import com.treode.async.Async
import com.treode.async.io.StubFile
import com.treode.cluster.{Cluster, HostId, StubActiveHost, StubHost, StubNetwork}
import com.treode.disk.{Disks, DisksConfig, DiskGeometry}
import com.treode.store.{Atlas, Catalogs, Cohort, Paxos, StoreConfig, TimedTestTools}
import com.treode.store.atlas.AtlasKit

import Async.guard
import TimedTestTools._

private class StubPaxosHost (id: HostId, network: StubNetwork)
extends StubActiveHost (id, network) {
  import network.{random, scheduler}

  implicit val cluster: Cluster = this

  implicit val disksConfig = TestDisksConfig()
  implicit val storeConfig = StoreConfig (4, 1<<16)

  implicit val recovery = Disks.recover()
  val _catalogs = Catalogs.recover()
  val _paxos = Paxos.recover()

  val file = new StubFile
  val geometry = TestDiskGeometry()
  val files = Seq ((Paths.get ("a"), file, geometry))

  val atlas = Atlas.recover (_catalogs) .asInstanceOf [AtlasKit]

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

  val acceptors = paxos.acceptors

  def setCohorts (cohorts: (StubHost, StubHost, StubHost)*) {
    val _cohorts =
      for ((h1, h2, h3) <- cohorts)
        yield Cohort.settled (h1.localId, h2.localId, h3.localId)
    atlas.set (_cohorts.toArray)
  }

  def issueCohorts (cohorts: (StubHost, StubHost, StubHost)*): Async [Unit] =
    guard {
      val _cohorts =
        for ((h1, h2, h3) <- cohorts)
          yield Cohort.settled (h1.localId, h2.localId, h3.localId)
      atlas.issue (_cohorts.toArray)
    }}
