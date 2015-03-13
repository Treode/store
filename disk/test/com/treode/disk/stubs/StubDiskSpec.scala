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

package com.treode.disk.stubs

import scala.util.Random

import com.treode.async.Async
import com.treode.async.stubs.implicits._
import com.treode.disk.{DiskTestTools, LogReplayer, LogTracker, StuffTracker}
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec

import Async.supply

class StubDiskSpec extends FreeSpec with CrashChecks {

  "The logger when" - {

    def check (checkpoint: Double, compaction: Double) (implicit random: Random) = {

      val checkpointing = checkpoint > 0.0
      val tracker = new LogTracker
      val drive = new StubDiskDrive
      var checkpointed = false

      setup { implicit scheduler =>
        implicit val config = StubDiskConfig (checkpoint, compaction)
        implicit val recovery = StubDisk.recover()
        implicit val launch = recovery.reattach (drive) .expectPass()
        import launch.disk
        tracker.attach()
        if (checkpointing)
          launch.checkpoint (supply (checkpointed = true))
        else
          launch.checkpoint (fail ("Expected no checkpoints"))
        launch.launch()
        tracker.batches (80, 40, 10)
      }

      .assert (!checkpointing || checkpointed, "Expected a checkpoint")

      .recover { implicit scheduler =>
        implicit val config = StubDiskConfig (checkpoint, compaction)
        implicit val recovery = StubDisk.recover()
        val replayer = new LogReplayer
        replayer.attach (recovery)
        implicit val disk = recovery.reattach (drive) .expectPass() .disk
        replayer.check (tracker)
      }}

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

        "should replay items" taggedAs (Intensive, Periodic) in {
          forAllCrashes { implicit random =>
            check (checkpoint, compaction)
          }}}}}

  "The pager when" - {

    def check (checkpoint: Double, compaction: Double) (implicit random: Random) = {

      val cleaning = compaction > 0.0
      val tracker = new StuffTracker
      val drive = new StubDiskDrive

      setup { implicit scheduler =>
        implicit val config = StubDiskConfig (checkpoint, compaction)
        implicit val recovery = StubDisk.recover()
        implicit val launch = recovery.reattach (drive) .expectPass()
        import launch.disk
        if (cleaning) tracker.attach()
        launch.launch()
        tracker.batch (40, 10)
      }

      .assert (!cleaning || tracker.probed && tracker.compacted, "Expected cleaning")

      .recover { implicit scheduler =>
        implicit val config = StubDiskConfig (checkpoint, compaction)
        implicit val recovery = StubDisk.recover()
        implicit val disk = recovery.reattach (drive) .expectPass() .disk
        tracker.check()
      }}

    for { (name, compaction) <- Seq (
        "not compacted at all"   -> 0.0,
        "compacted occasionally" -> 0.1,
        "compacted frequently"   -> 0.3)
    } s"$name should" - {

    "should read and write" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>
        check (0.0, compaction)
      }}}}}
