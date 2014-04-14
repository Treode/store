package com.treode.store.catalog

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
import Callback.ignore
import StubCatalogHost.{cat1, cat2}
import TimedTestTools._

private class StubCatalogHost (id: HostId, network: StubNetwork)
extends StubActiveHost (id, network) {
  import network.{random, scheduler}

  implicit val cluster: Cluster = this

  implicit val disksConfig = TestDisksConfig()
  implicit val storeConfig = TestStoreConfig()

  implicit val recovery = Disks.recover()
  val _catalogs = Catalogs.recover()

  var v1 = 0L
  var v2 = Seq.empty [Long]

  _catalogs.listen (cat1) (v1 = _)
  _catalogs.listen (cat2) (v2 = _)

  val file = new StubFile
  val geometry = TestDiskGeometry()
  val files = Seq ((Paths.get ("a"), file, geometry))

  val atlas = new AtlasKit

  val _launch =
    for {
      launch <- recovery.attach (files)
      catalogs <- _catalogs.launch (launch, atlas) .map (_.asInstanceOf [CatalogKit])
    } yield {
      launch.launch()
      (launch.disks, catalogs)
    }

  val captor = _launch.capture()
  scheduler.runTasks()
  while (!captor.wasInvoked)
    Thread.sleep (10)
  implicit val (disks, catalogs) = captor.passed

  val acceptors = catalogs.acceptors

  def setCohorts (cohorts: (StubHost, StubHost, StubHost)*) {
    val _cohorts =
      for (((h1, h2, h3), i) <- cohorts.zipWithIndex)
        yield Cohort.settled (i, h1.localId, h2.localId, h3.localId)
    atlas.set (_cohorts.toArray)
  }

  def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C) {
    import catalogs.broker.{diff, patch}
    patch (desc.id, diff (desc) (version, cat) .pass) .pass
  }}

private object StubCatalogHost {

  val cat1 = {
    import StorePicklers._
    CatalogDescriptor (0x07, fixedLong)
  }

  val cat2 = {
    import StorePicklers._
    CatalogDescriptor (0x7A, seq (fixedLong))
  }}
