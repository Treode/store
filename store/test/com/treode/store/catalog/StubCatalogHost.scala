/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.store.catalog

import scala.util.Random

import com.treode.async.{Async, Callback}
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.cluster.{Cluster, HostId}
import com.treode.cluster.stubs.{StubCluster, StubHost, StubNetwork}
import com.treode.disk.stubs.{StubDisk, StubDiskDrive}
import com.treode.store._
import org.scalatest.Assertions

import Assertions.assertResult
import Callback.ignore
import StubCatalogHost.{cat1, cat2}

private class StubCatalogHost (
    val localId: HostId
) (implicit
    random: Random,
    scheduler: StubScheduler,
    network: StubNetwork
) extends StubHost {

  val config = StoreTestConfig()
  import config._

  implicit val cluster = new StubCluster (localId)
  implicit val library = new Library
  implicit val recovery = StubDisk.recover()
  implicit val _catalogs = Catalogs.recover()

  val diskDrive = new StubDiskDrive

  var v1 = 0L
  var v2 = Seq.empty [Long]

  val _launch =
    for {
      launch <- recovery.reattach (diskDrive)
      catalogs <- _catalogs.launch (launch, cluster) .map (_.asInstanceOf [CatalogKit])
    } yield {
      launch.launch()
      catalogs.listen (cat1) (v1 = _)
      catalogs.listen (cat2) (v2 = _)
      (launch.disk, catalogs)
    }

  val captor = _launch.capture()
  scheduler.run()
  while (!captor.wasInvoked)
    Thread.sleep (10)
  implicit val (disk, catalogs) = captor.assertPassed()

  val acceptors = catalogs.acceptors

  cluster.startup()

  def setAtlas (cohorts: Cohort*) {
    val atlas = Atlas (cohorts.toArray, 1)
    library.atlas = atlas
    library.residents = atlas.residents (localId)
  }

  def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C) {
    import catalogs.broker.{diff, patch}
    patch (desc.id, diff (desc) (version, cat) .expectPass()) .expectPass()
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
