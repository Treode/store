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

import scala.util.Random

import com.treode.store.StoreTestConfig
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec

class AtomicSequentialSpec extends FreeSpec with AtomicBehaviors {

  "The atomic implementation should" - {

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

        for { (name, (ntables, nkeys)) <- Seq (
            "few tables and keys"   -> (3, 10),
            "many tables and keys"  -> (30, 100))
        } s"$name with" - {

          for { (name, (nbatches, nwrites, nops)) <- Seq (
              "batches with few ops" -> (3, 3, 3),
              "batches with many ops" -> (3, 3, 20))
          } name - {

            forAgentWithDeputyBouncing { implicit random =>
              issueAtomicWrites (nbatches, ntables, nkeys, nwrites, nops)
            }}}}}}

    "issue atomic writes with" - {

      implicit val config = StoreTestConfig()

      val init = { implicit random: Random =>
        issueAtomicWrites (3, 3, 100, 3, 3)
      }

      forVariousClusters (init)
    }

    "scan the whole databse with" - {

      for { (name, nslices) <- Seq (
          "one slice" -> 1,
          "four slices" -> 4)
      } s"$name and" - {

        implicit val config = StoreTestConfig()

        val init = { implicit random: Random =>
          scanWholeDatabase (nslices)
        }

        forVariousClusters (init)
      }}}}
