package com.treode.store

import java.nio.file.Paths

import com.treode.async.{Async, AsyncCaptor, AsyncChecks, StubScheduler}
import com.treode.async.io.StubFile
import com.treode.cluster.{Cluster, HostId, StubActiveHost, StubNetwork}
import com.treode.disk.Disks
import org.scalatest.FlatSpec

import Async.when
import StoreTestTools._

class LibrarianSpec extends FlatSpec with AsyncChecks {

  private class StubLibrarianHost (id: HostId, network: StubNetwork)
  extends StubActiveHost (id, network) {
    import network.{random, scheduler}

    implicit val cluster: Cluster = this
    implicit val library = new Library

    implicit val disksConfig = TestDisksConfig()
    implicit val storeConfig = TestStoreConfig()

    implicit val recovery = Disks.recover()
    implicit val _catalogs = Catalogs.recover()

    val file = new StubFile
    val geometry = TestDiskGeometry()
    val files = Seq ((Paths.get ("a"), file, geometry))

    val _launch =
      for {
        launch <- recovery.attach (files)
        catalogs <- _catalogs.launch (launch)
      } yield {
        launch.launch()
        (launch.disks, catalogs)
      }

    val captor = _launch.capture()
    scheduler.runTasks()
    while (!captor.wasInvoked)
      Thread.sleep (10)
    implicit val (disks, catalogs) = captor.passed

    val rebalancer = AsyncCaptor [Unit]

    def rebalance (atlas: Atlas): Async [Unit] = {
      val active = atlas.cohorts (0) contains localId
      val moving = atlas.cohorts exists (_.moving)
      when (active && moving) (rebalancer.start())
    }

    val librarian = new Librarian (rebalance _)

    scuttlebutt.attach (this)

    def issue (cohorts: Cohort*) {
      val version = library.atlas.version + 1
      val atlas = Atlas (cohorts.toArray, version)
      library.atlas = atlas
      library.residents = atlas.residents (localId)
      Atlas.catalog.issue (version, atlas) .pass
    }

    def expectAtlas (atlas: Atlas) {
      assertResult (atlas) (library.atlas)
      assertResult (librarian.issued) (atlas.version)
      assert (librarian.receipts forall (_._2 == atlas.version))
    }}

  def expectAtlas (version: Int, cohorts: Cohort*) (hosts: Seq [StubLibrarianHost]) {
    val atlas = Atlas (cohorts.toArray, version)
    for (host <- hosts)
      host.expectAtlas (atlas)
  }

  "It" should "work" in {
    val network = StubNetwork()
    val hs = network.install (10, new StubLibrarianHost (_, network))
    val Seq (h0, h1, h2, h3) = hs take 4

    for (h1 <- hs; h2 <- hs)
      h1.hail (h2.localId, null)
    h0.issue (issuing (h0, h1, h2) (h0, h1, h3))
    network.runTasks (count = 2000, timers = true)
    expectAtlas (2, moving (h0, h1, h2) (h0, h1, h3)) (hs)
    h0.rebalancer.pass()
    network.runTasks (count = 1000, timers = true)
    expectAtlas (2, moving (h0, h1, h2) (h0, h1, h3)) (hs)
    h1.rebalancer.pass()
    network.runTasks (count = 1000, timers = true)
    expectAtlas (2, moving (h0, h1, h2) (h0, h1, h3)) (hs)
    h2.rebalancer.pass()
    network.runTasks (count = 2000, timers = true)
    expectAtlas (3, settled (h0, h1, h3)) (hs)
  }}
