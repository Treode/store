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

package com.treode.disk

import java.util.logging.{Level, Logger}
import scala.util.Random

import com.treode.async._, Async.{async, latch}
import com.treode.async.implicits._
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.{AsyncCaptor, StubScheduler, StubGlobals}
import com.treode.async.stubs.implicits._
import com.treode.disk.stubs.CrashChecks
import com.treode.pickle.{InvalidTagException, Picklers}
import com.treode.tags.Periodic
import org.scalatest.FlatSpec

import DiskTestTools._

class LogSpec extends FlatSpec with CrashChecks {

  Logger.getLogger ("com.treode") .setLevel (Level.WARNING)

  class DistinguishedException extends Exception

  implicit val config = DiskTestConfig()
  val geom = DriveGeometry.test()

  object records {
    val str = RecordDescriptor (0xBF, Picklers.string)
    val stuff = RecordDescriptor (0x2B, Stuff.pickler)
  }

  "The logger" should "replay zero items" in {

    var file: StubFile = null

    {
      implicit val scheduler = StubScheduler.random()
      file = StubFile (1<<20, geom.blockBits)
      implicit val recovery = Disk.recover()
      recovery.attachAndLaunch (("a", file, geom))
    }

    {
      implicit val scheduler = StubScheduler.random()
      file = StubFile (file.data, geom.blockBits)
      implicit val recovery = Disk.recover()
      val replayed = Seq.newBuilder [String]
      records.str.replay (replayed += _)
      recovery.reattachAndLaunch (("a", file))
      assertResult (Seq.empty) (replayed.result)
    }}

  it should "replay one item" in {

    var file: StubFile = null

    {
      implicit val scheduler = StubScheduler.random()
      file = StubFile (1<<20, geom.blockBits)
      implicit val recovery = Disk.recover()
      implicit val disk = recovery.attachAndLaunch (("a", file, geom))
      records.str.record ("one") .expectPass()
    }

    {
      implicit val scheduler = StubScheduler.random()
      file = StubFile (file.data, geom.blockBits)
      implicit val recovery = Disk.recover()
      val replayed = Seq.newBuilder [String]
      records.str.replay (replayed += _)
      recovery.reattachAndLaunch (("a", file))
      assertResult (Seq ("one")) (replayed.result)
    }}

  it should "replay three items" in {

    var file: StubFile = null

    {
      implicit val scheduler = StubScheduler.random()
      file = StubFile (1<<20, geom.blockBits)
      implicit val recovery = Disk.recover()
      implicit val disk = recovery.attachAndLaunch (("a", file, geom))
      records.str.record ("one") .expectPass()
      records.str.record ("two") .expectPass()
      records.str.record ("three") .expectPass()
    }

    {
      implicit val scheduler = StubScheduler.random()
      file = StubFile (file.data, geom.blockBits)
      implicit val recovery = Disk.recover()
      val replayed = Seq.newBuilder [String]
      records.str.replay (replayed += _)
      recovery.reattachAndLaunch (("a", file))
      assertResult (Seq ("one", "two", "three")) (replayed.result)
    }}

  it should "replay twice" in {

    var file: StubFile = null

    {
      implicit val scheduler = StubScheduler.random()
      file = StubFile (1<<20, geom.blockBits)
      implicit val recovery = Disk.recover()
      implicit val disk = recovery.attachAndLaunch (("a", file, geom))
      records.str.record ("one") .expectPass()
    }

    {
      implicit val scheduler = StubScheduler.random()
      file = StubFile (file.data, geom.blockBits)
      implicit val recovery = Disk.recover()
      val replayed = Seq.newBuilder [String]
      records.str.replay (replayed += _)
      implicit val disk = recovery.reattachAndLaunch (("a", file))
      assertResult (Seq ("one")) (replayed.result)
      records.str.record ("two") .expectPass()
    }

    {
      implicit val scheduler = StubScheduler.random()
      file = StubFile (file.data, geom.blockBits)
      implicit val recovery = Disk.recover()
      val replayed = Seq.newBuilder [String]
      records.str.replay (replayed += _)
      recovery.reattachAndLaunch (("a", file))
      assertResult (Seq ("one", "two")) (replayed.result)
    }}

  it should "report an unrecognized record" in {

    var file: StubFile = null

    {
      implicit val scheduler = StubScheduler.random()
      file = StubFile (1<<20, geom.blockBits)
      implicit val recovery = Disk.recover()
      implicit val disk = recovery.attachAndLaunch (("a", file, geom))
      records.str.record ("one") .expectPass()
    }

    {
      implicit val scheduler = StubScheduler.random()
      file = StubFile (file.data, geom.blockBits)
      implicit val recovery = Disk.recover()
      recovery.reattachAndWait (("a", file)) .fail [InvalidTagException]
    }}

  it should "report an error from a replay function" in {

    var file: StubFile = null

    {
      implicit val scheduler = StubScheduler.random()
      file = StubFile (1<<20, geom.blockBits)
      implicit val recovery = Disk.recover()
      implicit val disk = recovery.attachAndLaunch (("a", file, geom))
      records.str.record ("one") .expectPass()
    }

    {
      implicit val scheduler = StubScheduler.random()
      file = StubFile (file.data, geom.blockBits)
      implicit val recovery = Disk.recover()
      records.str.replay (_ => throw new DistinguishedException)
      recovery.reattachAndWait (("a", file)) .fail [DistinguishedException]
    }}

  it should "reject an oversized record" in {

    {
      implicit val scheduler = StubScheduler.random()
      val file = StubFile (1<<20, geom.blockBits)
      implicit val recovery = Disk.recover()
      implicit val disk = recovery.attachAndLaunch (("a", file, geom))
      records.stuff.record (Stuff (0, 1000)) .fail [OversizedRecordException]
    }}


  it should "run one checkpoint at a time" taggedAs (Periodic) in {
    forAllRandoms { implicit random =>

      implicit val scheduler = StubScheduler.random (random)
      val file = StubFile (1<<20, geom.blockBits)
      val recovery = Disk.recover()
      val launch = recovery.attachAndWait (("a", file, geom)) .expectPass()
      import launch.disk

      var checkpointed = false
      var checkpointing = false
      launch.checkpoint (async [Unit] { cb =>
        assert (!checkpointing, "Expected one checkpoint at a time.")
        scheduler.execute {
          checkpointing = false
          cb.pass (())
        }
        checkpointed = true
        checkpointing = true
      })
      launch.launch()
      scheduler.run()

      latch (
          disk.checkpoint(),
          disk.checkpoint(),
          disk.checkpoint()) .expectPass()
      assert (checkpointed, "Expected a checkpoint")
    }}

  it should "restart a checkpoint after a crash" taggedAs (Periodic) in {
    forAllRandoms { implicit random =>

      var file: StubFile = null

      // Record a few log entries.
      {
        implicit val scheduler = StubScheduler.random()
        file = StubFile (1<<20, geom.blockBits)
        implicit val recovery = Disk.recover()
        implicit val disk = recovery.attachAndLaunch (("a", file, geom))
        records.str.record ("one") .expectPass()
        records.str.record ("two") .expectPass()
        records.str.record ("three") .expectPass()
      }

      // Restart with a low threshold, replays should trigger a checkpoint.
      {
        implicit val scheduler = StubScheduler.random()
        implicit val config = DiskTestConfig (checkpointEntries = 1)
        val captor = AsyncCaptor [Unit]

        file = StubFile (file.data, geom.blockBits)
        implicit val recovery = Disk.recover()
        records.str.replay (_ => ())
        val launch = recovery.reattachAndWait (("a", file)) .expectPass()
        launch.checkpoint (captor.start())
        launch.launch()
        scheduler.run()
        assertResult (1) (captor.outstanding)
      }}}}
