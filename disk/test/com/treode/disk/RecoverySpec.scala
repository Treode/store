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

import java.nio.file.Paths
import java.util.logging.{Level, Logger}

import com.treode.async.implicits._
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import org.scalatest.FreeSpec

import DiskTestTools._

class RecoverySpec extends FreeSpec {

  Logger.getLogger ("com.treode") .setLevel (Level.WARNING)

  implicit val config = DiskTestConfig()
  val geom = DriveGeometry.test()
  val record = RecordDescriptor (0x1BF6DBABE6A70060L, DiskPicklers.int)

  "Recovery.replay should" - {

    "allow registration of a record descriptor" in {
      implicit val scheduler = StubScheduler.random()
      val recovery = Disk.recover()
      recovery.replay (record) (_ => ())
    }

    "reject double registration of a record descriptor" in {
      implicit val scheduler = StubScheduler.random()
      val recovery = Disk.recover()
      recovery.replay (record) (_ => ())
      intercept [IllegalArgumentException] {
        recovery.replay (record) (_ => ())
      }}

    "reject registration of a record descriptor after attach" in {
      implicit val scheduler = StubScheduler.random()
      val file = StubFile (1<<20, geom.blockBits)
      val recovery = Disk.recover()
      recovery.attachAndWait (("a", file, geom)) .expectPass()
      intercept [IllegalArgumentException] {
        recovery.replay (record) (_ => ())
      }}

    "reject registration of a record descriptor after reattach" in {
      implicit val scheduler = StubScheduler.random()
      val file = StubFile (1<<20, geom.blockBits)
      var recovery = Disk.recover()
      recovery.attachAndLaunch (("a", file, geom))
      recovery = Disk.recover()
      recovery.reattachAndLaunch (("a", file))
      intercept [IllegalArgumentException] {
        recovery.replay (record) (_ => ())
      }}}

  "Recovery.attach should" - {

    "allow attaching an item" in {
      implicit val scheduler = StubScheduler.random()
      val file = StubFile (1<<20, geom.blockBits)
      val recovery = Disk.recover()
      recovery.attachAndLaunch (("a", file, geom))
    }

    "allow attaching multiple items" in {
      implicit val scheduler = StubScheduler.random()
      val file1 = StubFile (1<<20, geom.blockBits)
      val file2 = StubFile (1<<20, geom.blockBits)
      val recovery = Disk.recover()
      recovery.attachAndLaunch (("a", file1, geom), ("b", file2, geom))
    }

    "reject attaching no items" in {
      implicit val scheduler = StubScheduler.random()
      val recovery = Disk.recover()
      recovery.attachAndWait() .expectFail [IllegalArgumentException]
    }

    "reject attaching the same path multiply" in {
      implicit val scheduler = StubScheduler.random()
      val file = StubFile (1<<20, geom.blockBits)
      val recovery = Disk.recover()
      recovery
          .attachAndWait (("a", file, geom), ("a", file, geom))
          .expectFail [IllegalArgumentException]
    }

    "pass through an exception from DiskDrive.init" in {
      implicit val scheduler = StubScheduler.random()
      val file = StubFile (1<<20, geom.blockBits)
      val recovery = Disk.recover()
      file.stop = true
      val cb = recovery.attachAndCapture (("a", file, geom))
      scheduler.run()
      file.stop = false
      while (file.hasLast)
        file.last.fail (new Exception)
      scheduler.run()
      cb.assertFailed [Exception]
    }}

  "Recovery.reattach" - {

    "in general should" - {

      "allow reattaching an item" in {
        implicit val scheduler = StubScheduler.random()
        val file = StubFile (1<<20, geom.blockBits)
        var recovery = Disk.recover()
        recovery.attachAndLaunch (("a", file, geom))
        recovery = Disk.recover()
        recovery.reattachAndLaunch (("a", file))
      }

      "allow reattaching multiple items" in {
        implicit val scheduler = StubScheduler.random()
        val file1 = StubFile (1<<20, geom.blockBits)
        val file2 = StubFile (1<<20, geom.blockBits)
        var recovery = Disk.recover()
        recovery.attachAndLaunch (("a", file1, geom), ("b", file2, geom))
        recovery = Disk.recover()
        recovery.reattachAndLaunch (("a", file1), ("b", file2))
      }

      "reject reattaching no items" in {
        implicit val scheduler = StubScheduler.random()
        val recovery = Disk.recover()
        recovery.reattachAndWait() .expectFail [IllegalArgumentException]
      }

      "reject reattaching the same path multiply" in {
        implicit val scheduler = StubScheduler.random()
        val file = StubFile (1<<20, geom.blockBits)
        var recovery = Disk.recover()
        recovery.attachAndLaunch (("a", file, geom))
        recovery = Disk.recover()
        recovery
            .reattachAndWait (("a", file), ("a", file))
            .expectFail [IllegalArgumentException]
      }

      "pass through an exception from chooseSuperBlock" in {
        implicit val scheduler = StubScheduler.random()
        val file = StubFile (1<<20, geom.blockBits)
        val recovery = Disk.recover()
        recovery.reattachAndWait (("a", file)) .expectFail [NoSuperBlocksException]
      }

      "pass through an exception from verifyReattachment" in {
        implicit val scheduler = StubScheduler.random()
        val file1 = StubFile (1<<20, geom.blockBits)
        val file2 = StubFile (1<<20, geom.blockBits)
        var recovery = Disk.recover()
        recovery.attachAndLaunch (("a", file1, geom), ("b", file2, geom))
        recovery = Disk.recover ()
        recovery.reattachAndWait (("a", file1)) .expectFail [MissingDisksException]

      }}

    "when given opened files should" - {

      "require the given file paths match the boot blocks disk" in {
        implicit val scheduler = StubScheduler.random()
        val file = StubFile (1<<20, geom.blockBits)
        val file2 = StubFile (1<<20, geom.blockBits)
        var recovery = Disk.recover()
        recovery.attachAndLaunch (("a", file, geom), ("b", file2, geom))
        recovery = Disk.recover()
        recovery.reattachAndWait (("a", file)) .expectFail [MissingDisksException]
      }}

    "when given unopened paths should" - {

      "pass when given all of the items" in {
        implicit val scheduler = StubScheduler.random()
        val file = StubFile (1<<20, geom.blockBits)
        val file2 = StubFile (1<<20, geom.blockBits)
        var recovery = Disk.recover()
        recovery.attachAndLaunch (("a", file, geom), ("b", file2, geom))
        recovery = Disk.recover()
        recovery.reopenAndLaunch ("a", "b") (("a", file), ("b", file2))
      }

      "pass when given a subset of the items" in {
        implicit val scheduler = StubScheduler.random()
        val file = StubFile (1<<20, geom.blockBits)
        val file2 = StubFile (1<<20, geom.blockBits)
        var recovery = Disk.recover()
        recovery.attachAndLaunch (("a", file, geom), ("b", file2, geom))
        recovery = Disk.recover()
        recovery.reopenAndLaunch ("a") (("a", file), ("b", file2))
      }

      "fail when given extra uninitialized items" in {
        implicit val scheduler = StubScheduler.random()
        val file = StubFile (1<<20, geom.blockBits)
        val file2 = StubFile (1<<20, geom.blockBits)
        val file3 = StubFile (1<<20, geom.blockBits)
        var recovery = Disk.recover()
        recovery.attachAndLaunch (("a", file, geom), ("b", file2, geom))
        recovery = Disk.recover()
        recovery
            .reopenAndWait ("a", "c") (("a", file), ("b", file2), ("c", file3))
            .expectFail [InconsistentSuperBlocksException]
      }

      "fail when given extra initialized items" in {
        implicit val scheduler = StubScheduler.random()
        val file = StubFile (1<<20, geom.blockBits)
        val file2 = StubFile (1<<20, geom.blockBits)
        val file3 = StubFile (1<<20, geom.blockBits)
        var recovery = Disk.recover()
        recovery.attachAndLaunch (("a", file, geom), ("b", file2, geom))
        recovery = Disk.recover()
        recovery.attachAndLaunch (("c", file3, geom))
        recovery = Disk.recover()
        recovery
            .reopenAndWait ("a", "c") (("a", file), ("b", file2), ("c", file3))
            .expectFail [ExtraDisksException]
      }}}}
