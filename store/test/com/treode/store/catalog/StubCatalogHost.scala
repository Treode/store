package com.treode.store.catalog

import scala.util.Random

import com.treode.async.{Async, Callback}
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.implicits._
import com.treode.cluster.{Cluster, HostId}
import com.treode.cluster.stubs.StubCluster
import com.treode.disk.stubs.{StubDisks, StubDiskDrive}
import com.treode.store._
import org.scalatest.Assertions

import Assertions.assertResult
import Callback.ignore
import CatalogTestTools._
import StubCatalogHost.{cat1, cat2}

private class StubCatalogHost (val localId: HostId) (implicit kit: StoreTestKit)
extends StubStoreHost {
  import kit._

  implicit val cluster = new StubCluster (localId)

  implicit val library = new Library

  implicit val storeConfig = TestStoreConfig()
  implicit val recovery = StubDisks.recover()
  implicit val _catalogs = Catalogs.recover()

  val diskDrive = new StubDiskDrive

  var v1 = 0L
  var v2 = Seq.empty [Long]

  val _launch =
    for {
      launch <- recovery.attach (diskDrive)
      catalogs <- _catalogs.launch (launch) .map (_.asInstanceOf [CatalogKit])
    } yield {
      launch.launch()
      catalogs.listen (cat1) (v1 = _)
      catalogs.listen (cat2) (v2 = _)
      (launch.disks, catalogs)
    }

  val captor = _launch.capture()
  scheduler.run()
  while (!captor.wasInvoked)
    Thread.sleep (10)
  implicit val (disks, catalogs) = captor.passed

  val acceptors = catalogs.acceptors

  cluster.startup()

  def setAtlas (cohorts: Cohort*) {
    val atlas = Atlas (cohorts.toArray, 1)
    library.atlas = atlas
    library.residents = atlas.residents (localId)
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
