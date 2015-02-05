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

import java.util.concurrent.Executors
import scala.util.Random

import com.treode.async.stubs.{AsyncChecks, CallbackCaptor, StubGlobals, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.cluster.stubs.StubNetwork
import com.treode.store._
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.{BeforeAndAfterAll, FreeSpec, PropSpec, Suites}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.SpanSugar

import AtomicTestTools._
import SpanSugar._

class AtomicSpec extends FreeSpec with StoreBehaviors with AsyncChecks with TimeLimitedTests {

  override val timeLimit = 15 minutes

  private def newStore () (implicit r: Random, s: StubScheduler, n: StubNetwork): Store = {
    val hs = Seq.fill (3) (StubAtomicHost.install() .expectPass())
    val Seq (h1, h2, h3) = hs
    for (h <- hs)
      h.setAtlas (settled (h1, h2, h3))
    new TestableCluster (hs)
  }

  "The atomic implementation should" - {

    behave like aStore { (r, n, s) =>
      newStore () (r, n, s)
    }

    "conserve money during account transfers" taggedAs (Intensive, Periodic) in {
      forAllRandoms { random =>
        implicit val (r, s, n) = newKit()
        implicit val store = newStore()
        testAccountTransfers (100)
      }}

    "conserve money during account transfers (multithreaded)" taggedAs (Intensive, Periodic) in {
      import StubGlobals.scheduler
      implicit val random = Random
      implicit val network = StubNetwork (random)
      implicit val store = newStore()
      testAccountTransfers (100)
    }}}
