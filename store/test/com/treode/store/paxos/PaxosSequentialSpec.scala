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
import com.treode.store.{Bytes, StoreTestConfig, StoreTestKit}
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec

class PaxosSequentialSpec extends FreeSpec with PaxosBehaviors {

  "The paxos implementation should" - {

    "recover from a crash when" - {

      for { (name, checkpoint) <- Seq (
          "not checkpointed at all"   -> 0.0,
          "checkpointed occasionally" -> 0.01,
          "checkpointed frequently"   -> 0.1)
      } s"$name and" - {

        for { (name, compaction) <- Seq (
            "not compacted at all"   -> 0.0,
            "compacted occasionally" -> 0.01,
            "compacted frequently"   -> 0.1)
            if checkpoint >= compaction
      } s"$name with" - {

        implicit val config = StoreTestConfig (
            checkpointProbability = checkpoint,
            compactionProbability = compaction)

        for { (name, (nbatch, nputs)) <- Seq (
            "some batches"     -> (10, 10),
            "lots of batches"  -> (100, 10),
            "some big batches" -> (10, 100))
        } name taggedAs (Intensive, Periodic) in {

          forAllCrashes { implicit random =>
            crashAndRecover (nbatch, nputs)
          }}}}}

    "achieve consensus with" - {

      implicit val config = StoreTestConfig()

      val init = { implicit random: Random =>
        achieveConsensus (10, 10)
      }

      for1host (init)
      for3hosts (init)
      for8hosts (init)
      for3with1offline (init)
      for3with1crashing (init)
      for3with1rebooting (init)
      for3with1bouncing (init)
      for1to1 (init)
      for1to3 (init)
      for1to3with1bouncing (init)
      for3to1 (init)
      for3to1with1bouncing (init)
      for3replacing1 (init)
      for3replacing1withSourceBouncing (init)
      for3replacing1withTargetBouncing (init)
      for3replacing1withCommonBouncing (init)
      for3replacing2 (init)
      for3replacing2withSourceBouncing (init)
      for3replacing2withTargetBouncing (init)
      for3replacing2withCommonBouncing (init)
      for3to3 (init)
      for3to3withSourceBouncing (init)
      for3to3withTargetBouncing (init)
      for3to8 (init)
      for8to3 (init)
    }}}
