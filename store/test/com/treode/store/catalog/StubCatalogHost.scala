package com.treode.store.catalog

import java.nio.file.Paths
import scala.util.Random

import com.treode.async.{Async, AsyncTestTools, Callback, CallbackCaptor}
import com.treode.async.io.StubFile
import com.treode.cluster.{Cluster, HostId, StubActiveHost, StubNetwork}
import com.treode.store._
import com.treode.disk.{Disks, DisksConfig, DiskGeometry}
import org.scalatest.Assertions

import Assertions.expectResult
import AsyncTestTools._
import Callback.ignore
import StubCatalogHost.{cat1, cat2}

class StubCatalogHost (id: HostId, network: StubNetwork) extends StubActiveHost (id, network) {
  import network.{random, scheduler}

  implicit val cluster: Cluster = this

  implicit val disksConfig = DisksConfig (14, 1<<24, 1<<16, 10, 1)
  implicit val storeConfig = StoreConfig (8, 1<<16)

  implicit val recovery = Disks.recover()
  val _catalogs = Catalogs.recover()

  var v1 = 0L
  var v2 = Seq.empty [Long]

  _catalogs.listen (cat1) (v1 = _)
  _catalogs.listen (cat2) (v2 = _)

  val file = new StubFile
  val geometry = DiskGeometry (14, 8, 1<<20)
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

  def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Unit =
    catalogs.issue (desc) (version, cat)
}

object StubCatalogHost {


  val cat1 = {
    import StorePicklers._
    CatalogDescriptor (0x07, fixedLong)
  }

  val cat2 = {
    import StorePicklers._
    CatalogDescriptor (0x7A, seq (fixedLong))
  }}
