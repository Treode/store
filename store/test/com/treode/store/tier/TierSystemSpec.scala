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

package com.treode.store.tier

import scala.util.Random

import com.treode.async.Async
import com.treode.async.implicits._
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.disk.stubs.{CrashChecks, StubDisk, StubDiskDrive}
import com.treode.store.{Bytes, StoreTestConfig}
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec

import Async.async
import TierTestTools._

class TierSystemSpec extends FreeSpec with CrashChecks {

  val ID = 0xC8

  def crashAndRecover (
      nkeys: Int,
      nputs: Int,
      nbatches: Int
  ) (implicit
      random: Random,
      config: StoreTestConfig
  ) = {
    import config._

    val tracker = new TableTracker
    val diskDrive = new StubDiskDrive

    crash.info (s"check ($nkeys, $nputs, $nbatches, $config)")

    .setup { implicit scheduler =>
      implicit val recovery = StubDisk.recover()
      val medic = new TestMedic (ID)
      val launch = recovery.attach (diskDrive) .pass
      val rawtable = medic.launch (launch) .pass
      launch.launch()
      val table = new TrackedTable (rawtable, tracker)
      for (_ <- (0 until nbatches) .async)
        table.putAll (random.nextPut (nkeys, nputs): _*)
    }

    .recover { implicit scheduler =>
      implicit val recovery = StubDisk.recover()
      val medic = new TestMedic (ID)
      val launch = recovery.reattach (diskDrive) .pass
      val table = medic.launch (launch) .pass
      launch.launch()
      tracker.check (table.toMap)
    }}

  def writeManyTimes (
      nkeys: Int,
      nputs: Int,
      nbatches: Int
  ) (implicit
      random: Random,
      scheduler: StubScheduler,
      config: StoreTestConfig
  ) {
    import config._

    val tracker = new TableTracker
    val diskDrive = new StubDiskDrive

    implicit val recovery = StubDisk.recover()
    val medic = new TestMedic (ID)
    val launch = recovery.attach (diskDrive) .pass
    val rawtable = medic.launch (launch) .pass
    launch.launch()
    val table = new TrackedTable (rawtable, tracker)
    for (_ <- (0 until nbatches))
      table.putAll (random.nextPut (nkeys, nputs): _*) .pass
    tracker.check (rawtable.toMap)
  }

  "The TierTable when" - {

    for { (name, checkpoint) <- Seq (
        "not checkpointed at all"   -> 0.0,
        "checkpointed occasionally" -> 0.01,
        "checkpointed frequently"   -> 0.1)
    } s"$name and" - {

      for { (name, compaction) <- Seq (
          "not compacted at all"   -> 0.0,
          "compacted occasionally" -> 0.01,
          "compacted frequently"   -> 0.1)
    } s"$name should" - {

      implicit val storeConfig = StoreTestConfig (
          checkpointProbability = checkpoint,
          compactionProbability = compaction)

      for { (name, (nkeys, nputs, nbatches)) <- Seq (
          "recover from a crash"             -> (100, 10, 10),
          "recover with lots of overwrites"  -> (30, 10, 10),
          "recover with very few overwrites" -> (10000, 10, 10))
      } name taggedAs (Intensive, Periodic) in {

        forAllCrashes { implicit random =>
          crashAndRecover (nkeys, nputs, nbatches)
        }}

      for { (name, (nkeys, nputs, nbatches)) <- Seq (
          "write a lot" -> (100, 2, 2500))
      } name taggedAs (Intensive, Periodic) in { pending // too long

        forAllSeeds { implicit random =>
          implicit val scheduler = StubScheduler.random (random)
          writeManyTimes (nkeys, nputs, nbatches)
        }}}}}}
