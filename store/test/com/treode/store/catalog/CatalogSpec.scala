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

import com.treode.async.stubs.{AsyncChecks, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.cluster.stubs.StubNetwork
import com.treode.store._
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec

import CatalogTestTools._
import StubCatalogHost.cat1

class CatalogSpec extends FreeSpec with AsyncChecks {

  val val0 = Bytes.empty
  val val1 = Bytes (cat1.pcat, 0x9E587C3979DFCFFFL)
  val val2 = Bytes (cat1.pcat, 0x8041608E94F55C6DL)

  private val patch1 =
    Patch (1, val1.murmur32, Seq (Patch.diff (val0, val1)))

  private val patch2 =
    Patch (1, val2.murmur32, Seq (Patch.diff (val0, val2)))

  private class Summary (var timedout: Boolean, var chosen: Set [Update]) {

    def this() = this (false, Set.empty)

    def chose (v: Update): Unit =
      chosen += v

    def check (domain: Set [Update]) {
      val domain0 = domain + Update.empty
      assert (chosen forall (domain0 contains _))
    }}

  // Propose two patches simultaneously, expect one choice.
  def check (
      p1: StubCatalogHost,         // First host that will submit a proposal.
      p2: StubCatalogHost,         // Second host that will submit a proposal.
      as: Seq [StubCatalogHost],   // Hosts that we expect will accept.
      mf: Double,
      summary: Summary
  ) (implicit
      random: Random,
      scheduler: StubScheduler,
      network: StubNetwork
  ) {
    try {

      val cb1 = p1.catalogs.propose (cat1.id, patch1) .capture()
      val cb2 = p2.catalogs.propose (cat1.id, patch2) .capture()
      network.messageFlakiness = mf
      scheduler.run (timers = true, count = 500)
      val v = cb1.assertPassed()
      assertResult (v) (cb2.assertPassed())

      for (h <- as)
        assert (
            !h.catalogs.acceptors.acceptors.contains (cat1.id),
            "Expected acceptor to have been removed.")

      summary.chose (v)
    } catch {
      case e: TimeoutException =>
        summary.timedout = true
    }}

  "The acceptors should" - {

    "achieve consensus with" - {

      "stable hosts and a reliable network" taggedAs (Intensive, Periodic) in {
        var summary = new Summary
        forAllRandoms { implicit random =>
          implicit val scheduler = StubScheduler.random (random)
          implicit val network = StubNetwork (random)
          val hs = Seq.fill (3) (new StubCatalogHost (random.nextLong))
          val Seq (h1, h2, h3) = hs
          for (h <- hs)
            h.setAtlas (settled (h1, h2, h3))
          check (h1, h2, hs, 0.0, summary)
        }
        summary.check (Set (patch1, patch2))
      }

      "stable hosts and a flakey network" taggedAs (Intensive, Periodic) in {
        var summary = new Summary
        forAllRandoms { implicit random =>
          implicit val scheduler = StubScheduler.random (random)
          implicit val network = StubNetwork (random)
          val hs = Seq.fill (3) (new StubCatalogHost (random.nextLong))
          val Seq (h1, h2, h3) = hs
          for (h <- hs)
            h.setAtlas (settled (h1, h2, h3))
          check (h1, h2, hs, 0.1, summary)
        }
        summary.check (Set (patch1, patch2))
      }}}

  "The kit should" - {

    def setup (implicit random: Random = new Random (0)) = {
      implicit val scheduler = StubScheduler.random (random)
      implicit val network = StubNetwork (random)
      val hs = Seq.fill (3) (new StubCatalogHost (random.nextLong))
      val Seq (h1, h2, h3) = hs
      for (h <- hs)
        h.setAtlas (settled (h1, h2, h3))
      (scheduler, hs, h1, h2)
    }

    "distribute one issue of a catalog" in {
      forAllRandoms { random =>
        implicit val (scheduler, hs, h1, _) = setup (random)
        h1.catalogs.issue (cat1) (1, 0x658C1274DE7CFA8EL) .expectPass()
        scheduler.run (timers = true, count = 500)
        for (h <- hs)
          assertResult (0x658C1274DE7CFA8EL) (h.v1)
      }}

    "distribute two issues of a catalog, one after the other" in {
      forAllRandoms { random =>
        implicit val (scheduler, hs, h1, _) = setup (random)
        h1.catalogs.issue (cat1) (1, 0x658C1274DE7CFA8EL) .expectPass()
        h1.catalogs.issue (cat1) (2, 0x48B944DD188FD6D1L) .expectPass()
        scheduler.run (timers = true, count = 500)
        for (h <- hs)
          assertResult (0x48B944DD188FD6D1L) (h.v1)
      }}

    "reject a new issue when its version number is behind" in {
      implicit val (scheduler, hs, h1, h2) = setup()
      h1.catalogs.issue (cat1) (1, 0x658C1274DE7CFA8EL) .expectPass()
      scheduler.run (timers = true, count = 500)
      h2.catalogs.issue (cat1) (1, 0x1195296671067D1AL) .fail [StaleException]
      scheduler.run (timers = true, count = 500)
      for (h <- hs)
        assertResult (0x658C1274DE7CFA8EL) (h.v1)
    }

    "reject a new issue when its version number is ahead" in {
      implicit val (scheduler, hs, h1, h2) = setup()
      h1.catalogs.issue (cat1) (1, 0x658C1274DE7CFA8EL) .expectPass()
      scheduler.run (timers = true, count = 500)
      h2.catalogs.issue (cat1) (1, 0x1195296671067D1AL) .fail [StaleException]
      scheduler.run (timers = true, count = 500)
      for (h <- hs)
        assertResult (0x658C1274DE7CFA8EL) (h.v1)
    }}}
