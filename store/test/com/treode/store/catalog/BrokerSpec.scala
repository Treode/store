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
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.implicits._
import com.treode.cluster.{Cluster, HostId}
import com.treode.disk.stubs.{StubDisk, StubDiskDrive}
import com.treode.pickle.{Pickler, Picklers}
import com.treode.store._
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.{FreeSpec, PropSpec, Suites}
import org.scalatest.concurrent.TimeLimitedTests

import StubCatalogHost.{cat1, cat2}
import StoreTestTools._

class BrokerSpec extends Suites (BrokerBehaviors, new BrokerProperties)

object BrokerBehaviors extends FreeSpec {

  val ID1 = CatalogId (0xCB)
  val ID2 = CatalogId (0x2D)
  val cat1 = new CatalogDescriptor (ID1, Picklers.fixedLong)
  val cat2 = new CatalogDescriptor (ID2, Picklers.fixedLong)

  val values = Seq (
      0x292C28335A06E344L, 0xB58E76CED969A4C7L, 0xDF20D7F2B8C33B9EL, 0x63D5DAAF0C58D041L,
      0x834727637190788AL, 0x2AE35ADAA804CE32L, 0xE3AA9CFF24BC92DAL, 0xCE33BD811236E7ADL,
      0x7FAF87891BE9818AL, 0x3C15A9283BDFBA51L, 0xE8E45A575513FA90L, 0xE224EF2739907F79L,
      0xFC275E6C532CB3CBL, 0x40C505971288B2DDL, 0xCD1C2FD6707573E1L, 0x2D62491B453DA6A3L,
      0xA079188A7E0C8C39L, 0x46A5B2A69D90533AL, 0xD68C9A2FDAEE951BL, 0x7C781E5CF39A5EB1L)

  val bytes = values map (Bytes (_))

  val patches = {
    var prev = Bytes.empty
    for (v <- bytes) yield {
      val p = Patch.diff (prev, v)
      prev = v
      p
    }}

  private class RichBroker (implicit random: Random, scheduler: StubScheduler) {

    val config = StoreTestConfig()
    import config._

    val diskDrive = new StubDiskDrive

    implicit val recovery = StubDisk.recover()
    implicit val launch = recovery.attach (diskDrive) .expectPass()
    implicit val disk = launch.disk
    launch.launch()

    val broker = new Broker (Map.empty)

    def status = broker.status

    def ping (values: (CatalogId, Int)*) =
      broker.ping (values)

    def sync (other: RichBroker) {
      val task = for {
        status <- broker.status
        updates <- other.broker.ping (status)
      } yield broker.sync (updates)
      task.expectPass()
    }

    def double (other: RichBroker) {
      val task = for {
        status <- broker.status
        updates <- other.broker.ping (status)
      } yield {
        broker.sync (updates)
        broker.sync (updates)
      }
      task.expectPass()
    }

    def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any) =
      broker.listen (desc) (f)

    def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C) = {
      broker.patch (desc.id, broker.diff (desc) (version, cat) .expectPass()) .expectPass()
    }}

  private def newBroker = {
    implicit val random = new Random (0)
    implicit val scheduler = StubScheduler.random (random)
    val broker = new RichBroker
    (scheduler, broker)
  }

  private def newBrokers = {
    implicit val random = new Random (0)
    implicit val scheduler = StubScheduler.random (random)
    implicit val config = StoreTestConfig()
    val broker1 = new RichBroker
    val broker2 = new RichBroker
    (scheduler, broker1, broker2)
  }

  "When the broker is empty, it should" - {

    "yield an empty status" in {
      implicit val (scheduler, broker) = newBroker
      broker.status expectSeq ()
    }

    "yield empty updates on empty ping" in {
      implicit val (scheduler, broker) = newBroker
      broker.ping () expectSeq ()
    }

    "yield empty updates on non-empty ping" in {
      implicit val (scheduler, broker) = newBroker
      broker.ping (ID1 -> 12) expectSeq ()
    }}

  "When the broker has one local update, it should" - {

    "yield a non-empty status" in {
      implicit val (scheduler, broker) = newBroker
      broker.issue (cat1) (1, values (0))
      broker.status expectSeq (ID1 -> 1)
    }

    def patch (drop: Int, take: Int): Update = {
      val version  = take + drop
      val bytes = Bytes (values (version - 1))
      Patch (version, bytes.murmur32, patches drop (drop) take (take))
    }

    "yield non-empty deltas on empty ping" in {
      implicit val (scheduler, broker) = newBroker
      broker.issue (cat1) (1, values (0))
      broker.ping () expectSeq (ID1 -> patch (0, 1))
    }

    "yield non-empty deltas on ping missing the value" in {
      implicit val (scheduler, broker) = newBroker
      broker.issue (cat1) (1, values (0))
      broker.ping (ID2 -> 1) expectSeq (ID1 -> patch (0, 1))
    }

    "yield non-empty deltas on ping out-of-date with this host" in {
      implicit val (scheduler, broker) = newBroker
      broker.issue (cat1) (1, values (0))
      broker.issue (cat1) (2, values (1))
      broker.ping (ID1 -> 1) expectSeq (ID1 -> patch (1, 1))
    }

    "yield empty deltas on ping up-to-date with this host" in {
      implicit val (scheduler, broker) = newBroker
      broker.issue (cat1) (1, values (0))
      broker.ping (ID1 -> 1) expectSeq ()
    }}

  "When the broker receives a sync with one value, it should" - {

    "yield a status that contains the value" in {
      implicit val (scheduler, b1, b2) = newBrokers
      b1.issue (cat1) (1, values (0))
      b2.sync (b1)
      b2.status expectSeq (ID1 -> 1)
    }

    "invoke the listener on a first update" in {
      implicit val (scheduler, b1, b2) = newBrokers
      var v = 0L
      b2.listen (cat1) (v = _)
      b1.issue (cat1) (1, values (0))
      b2.sync (b1)
      assertResult (values (0)) (v)
    }

    "invoke the listener on second update" in {
      implicit val (scheduler, b1, b2) = newBrokers
      var v = 0L
      b2.listen (cat1) (v = _)
      b1.issue (cat1) (1, values (0))
      b2.sync (b1)
      b1.issue (cat1) (2, values (1))
      b2.sync (b1)
      assertResult (values (1)) (v)
    }

    "ignore a repeated update" in {
      implicit val (scheduler, b1, b2) = newBrokers
      var count = 0
      b2.listen (cat1) (_ => count += 1)
      b1.issue (cat1) (1, values (0))
      b2.double (b1)
      assertResult (1) (count)
    }
  }

  "When the broker receives a sync with two values it should" - {

    "invoke the listener once foreach value" in {
      implicit val (scheduler, b1, b2) = newBrokers
      var v1 = 0L
      var v2 = 0L
      b2.listen (cat1) (v1 = _)
      b2.listen (cat2) (v2 = _)
      b1.issue (cat1) (1, values (0))
      b1.issue (cat2) (1, values (1))
      b2.sync (b1)
      assertResult (values (0)) (v1)
      assertResult (values (1)) (v2)
    }}}

class BrokerProperties extends PropSpec with AsyncChecks {

  val values = BrokerBehaviors.values

  def checkUnity (random: Random, mf: Double) {
    implicit val (random, scheduler, network) = newKit()
    network.messageFlakiness = mf
    val hs = Seq.fill (3) (new StubCatalogHost (random.nextLong))
    for (h1 <- hs; h2 <- hs)
      h1.hail (h2.localId)
    scheduler.run()

    val vs1 = values
    val vs2 = vs1.updated (5, 0x4B00FB5F38430882L)
    val vs3 = vs2.updated (17, 0x8C999CB6054CCB61L)
    val vs4 = vs3.updated (11, 0x3F081D8657CD9220L)

    val Seq (h1, h2, h3) = hs
    h1.issue (cat1) (1, 0xED0F7511F6E3EC20L)
    h1.issue (cat2) (1, vs1)
    h1.issue (cat1) (2, 0x30F517CC57223260L)
    h1.issue (cat2) (2, vs2)
    h1.issue (cat1) (3, 0xC97846EBE5AC571BL)
    h1.issue (cat2) (3, vs3)
    h1.issue (cat1) (4, 0x4A048A835ED3A0A6L)
    h1.issue (cat2) (4, vs4)
    scheduler.run (timers = true, count = 400)

    for (h <- hs) {
      assertResult (0x4A048A835ED3A0A6L) (h.v1)
      assertResult (vs4) (h.v2)
    }}

  property ("The broker should distribute catalogs", Intensive, Periodic) {
    forAllRandoms { random =>
      checkUnity (random, 0.0)
    }}

  property ("The broker should distribute catalogs with a flakey network", Intensive, Periodic) {
    forAllRandoms { random =>
      checkUnity (random, 0.1)
    }}}
