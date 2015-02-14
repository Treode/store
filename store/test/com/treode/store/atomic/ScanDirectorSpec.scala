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

package com.treode.store.atomic

import scala.collection.SortedMap
import scala.util.Random

import com.treode.async.Async, Async.supply
import com.treode.async.stubs.{AsyncChecks, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.cluster.HostId
import com.treode.cluster.stubs.{StubCluster, StubHost, StubNetwork}
import com.treode.store._
import com.treode.tags.Periodic
import org.scalatest.FreeSpec

import AtomicTestTools._
import Fruits.AllFruits
import StubScanDeputy.fruitsId

class ScanDirectorSpec extends FreeSpec with AsyncChecks {

  private implicit class RichStubAtomicHost (host: StubAtomicHost) {

    /** Scan the fruits table, yield only the keys. */
    def scanFruits () (implicit s: StubScheduler): Seq [Bytes] =
      host
        .scan (fruitsId, 0, Bound.firstKey, Window.all, Slice.all, Batch.suggested)
        .map (_.key)
        .toSeq
        .expectPass()
  }

  "The ScanDirector should" - {

    "scan from one replica" taggedAs (Periodic)  in {
      forAllRandoms { implicit random =>
        implicit val scheduler = StubScheduler.random (random)
        implicit val network = StubNetwork (random)
        val h = StubAtomicHost.install() .expectPass()
        val d = StubScanDeputy.install() .expectPass()
        h.setAtlas (settled (d))
        assertResult (AllFruits) (h.scanFruits())
      }}

    "scan from three replicas" taggedAs (Periodic) in {
      forAllRandoms { implicit random =>
        implicit val scheduler = StubScheduler.random (random)
        implicit val network = StubNetwork (random)
        val h = StubAtomicHost.install() .expectPass()
        val d1 = StubScanDeputy.install() .expectPass()
        val d2 = StubScanDeputy.install() .expectPass()
        val d3 = StubScanDeputy.install() .expectPass()
        h.setAtlas (settled (d1, d2, d3))
        assertResult (AllFruits) (h.scanFruits())
      }}}}
