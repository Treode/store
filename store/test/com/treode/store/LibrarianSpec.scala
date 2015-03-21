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

package com.treode.store

import scala.util.Random

import com.treode.async.Async
import com.treode.async.stubs.StubScheduler
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.{AsyncCaptor, AsyncChecks, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.cluster.{Cluster, HostId}
import com.treode.cluster.stubs.{StubCluster, StubHost, StubNetwork}
import com.treode.disk.stubs.{StubDisk, StubDiskDrive}
import com.treode.store.catalog.Catalogs
import org.scalatest.FlatSpec

import Async.when
import StoreTestTools._

class LibrarianSpec extends FlatSpec with AsyncChecks {

  private class StubLibrarianHost (
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

    val _launch =
      for {
        launch <- recovery.reattach (diskDrive)
        catalogs <- _catalogs.launch (launch, cluster)
      } yield {
        launch.launch()
        (launch.disk, catalogs)
      }

    val captor = _launch.capture()
    scheduler.run()
    while (!captor.wasInvoked)
      Thread.sleep (10)
    implicit val (disk, catalogs) = captor.assertPassed()

    val rebalancer = AsyncCaptor [Unit]

    def rebalance (atlas: Atlas): Async [Unit] = {
      val active = atlas.cohorts (0) contains localId
      val moving = atlas.cohorts exists (_.moving)
      when (active && moving) (rebalancer.start())
    }

    val librarian = Librarian (rebalance _)

    cluster.startup()

    def issue (cohorts: Cohort*) {
      val version = library.atlas.version + 1
      val atlas = Atlas (cohorts.toArray, version)
      library.atlas = atlas
      library.residents = atlas.residents (localId)
      catalogs.issue (Atlas.catalog) (version, atlas) .expectPass()
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

    implicit val (random, scheduler, network) = newKit()

    val hs = Seq.fill (10) (new StubLibrarianHost (random.nextLong))
    val Seq (h0, h1, h2, h3) = hs take 4

    for (h1 <- hs; h2 <- hs)
      h1.hail (h2.localId)
    h0.issue (issuing (h0, h1, h2) (h0, h1, h3))
    scheduler.run (count = 2000, timers = true)
    expectAtlas (2, moving (h0, h1, h2) (h0, h1, h3)) (hs)
    h0.rebalancer.pass (())
    scheduler.run (count = 2000, timers = true)
    expectAtlas (2, moving (h0, h1, h2) (h0, h1, h3)) (hs)
    h1.rebalancer.pass (())
    scheduler.run (count = 2000, timers = true)
    expectAtlas (3, settled (h0, h1, h3)) (hs)
  }}
