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

package com.treode.store.paxos

import scala.util.Random

import com.treode.cluster.stubs.StubNetwork
import com.treode.disk.stubs.StubDiskDrive
import com.treode.store.{Bytes, StoreTestConfig}
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec

class PaxosSequentialSpec extends FreeSpec with PaxosBehaviors {

  "The paxos implementation should" - {

    "achieve consensus with" - {

      implicit val config = StoreTestConfig()

      val init = { implicit random: Random =>
        achieveConsensus (10, 10)
      }

      forVariousClusters (init)
    }}}
