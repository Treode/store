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

package com.treode.store.stubs

import java.util.concurrent.Executors
import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.async.stubs.{AsyncChecks, StubGlobals, StubScheduler}
import com.treode.tags.{Intensive, Periodic}
import com.treode.store._
import org.scalatest.FreeSpec

class StubStoreSpec extends FreeSpec with AsyncChecks with StoreBehaviors {

  "The StubStore should" - {

    behave like aStore {
      (random, scheduler, network) =>
        new StubStore () (scheduler)
    }

    "conserve money during account transfers (multithreaded)" taggedAs (Intensive, Periodic) in {
      import StubGlobals.scheduler
      implicit val random = Random
      implicit val store = new StubStore () (scheduler)
      testAccountTransfers (500)
    }}}
