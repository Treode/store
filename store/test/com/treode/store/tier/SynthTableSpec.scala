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

import java.nio.file.Paths
import scala.util.Random

import com.treode.async.Async
import com.treode.async.implicits._
import com.treode.async.io.File
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.{CallbackCaptor, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.disk.{GroupId, ObjectId, PageHandler}
import com.treode.disk.stubs.{StubDisk, StubDiskDrive}
import com.treode.store.{Fruits, Residents, StoreTestConfig}
import com.treode.pickle.Picklers
import org.scalatest.FreeSpec

import Async.guard
import Fruits.{Grape, Kiwi, Orange}
import TierTable.Checkpoint
import TierTestTools._

class SynthTableSpec extends FreeSpec {

  val tier = TierDescriptor (0x56) ((_, _, _) => true)

  class TierHandler (table: SynthTable) extends PageHandler {

    def probe (obj: ObjectId, groups: Set [GroupId]): Async [Set [GroupId]] = guard {
      table.probe (groups)
    }

    def compact (obj: ObjectId, groups: Set [GroupId]): Async [Unit] = guard {
      for {
        _ <- table.compact (groups, Residents.all)
      } yield ()
    }

    def checkpoint(): Async [Unit] = guard {
      for {
        _ <- table.checkpoint (Residents.all)
      } yield ()
    }}

  private def mkTable (diskDrive: StubDiskDrive) (
      implicit random: Random, scheduler: StubScheduler): SynthTable = {
    val config = StoreTestConfig (checkpointProbability = 0.0, compactionProbability = 0.0)
    import config._
    implicit val recovery = StubDisk.recover()
    implicit val launch = recovery.attach (diskDrive) .expectPass()
    import launch.disk
    val table = SynthTable (tier, 0x62)
    tier.handle (new TierHandler (table))
    launch.launch()
    table
  }

  private def aNonEmptyTable (setup: (Random, StubScheduler) => SynthTable) {

    "iterate and get those values" in {
      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val table = setup (random, scheduler)
      table.check (Kiwi##21::3, Kiwi##14::2, Kiwi##7::1)
    }

    "put a new key before existing keys" in {
      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val table = setup (random, scheduler)
      table.putCells (Grape##7::11)
      table.check (Grape##7::11, Kiwi##21::3, Kiwi##14::2, Kiwi##7::1)
    }

    "put a new key after existing keys" in {
      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val table = setup (random, scheduler)
      table.putCells (Orange##7::11)
      table.check (Kiwi##21::3, Kiwi##14::2, Kiwi##7::1, Orange##7::11)
    }

    "put an existing key at a time later than exsiting times" in {
      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val table = setup (random, scheduler)
      table.putCells (Kiwi##28::11)
      table.check (Kiwi##28::11, Kiwi##21::3, Kiwi##14::2, Kiwi##7::1)
    }

    "put an existing key at a time earlier than existing times" in {
      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val table = setup (random, scheduler)
      table.putCells (Kiwi##1::11)
      table.check (Kiwi##21::3, Kiwi##14::2, Kiwi##7::1, Kiwi##1::11)
    }

    "put an existing key at a time between two existing times" in {
      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val table = setup (random, scheduler)
      table.putCells (Kiwi##11::11)
      table.check (Kiwi##21::3, Kiwi##14::2, Kiwi##11::11, Kiwi##7::1)
    }

    "put an existing key over an existing time" in {
      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val table = setup (random, scheduler)
      table.putCells (Kiwi##14::11)
      table.check (Kiwi##21::3, Kiwi##14::11, Kiwi##7::1)
    }

    "delete a new key before existing keys" in {
      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val table = setup (random, scheduler)
      table.deleteCells (Grape##7)
      table.check (Grape##7, Kiwi##21::3, Kiwi##14::2, Kiwi##7::1)
    }

    "delete a new key after existing keys" in {
      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val table = setup (random, scheduler)
      table.deleteCells (Orange##7)
      table.check (Kiwi##21::3, Kiwi##14::2, Kiwi##7::1, Orange##7)
    }

    "delete an existing key at a time later than exsiting times" in {
      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val table = setup (random, scheduler)
      table.deleteCells (Kiwi##28)
      table.check (Kiwi##28, Kiwi##21::3, Kiwi##14::2, Kiwi##7::1)
    }

    "delete an existing key at a time earlier than existing times" in {
      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val table = setup (random, scheduler)
      table.deleteCells (Kiwi##1)
      table.check (Kiwi##21::3, Kiwi##14::2, Kiwi##7::1, Kiwi##1)
    }

    "delete an existing key at a time between two existing times" in {
      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val table = setup (random, scheduler)
      table.deleteCells (Kiwi##11)
      table.check (Kiwi##21::3, Kiwi##14::2, Kiwi##11, Kiwi##7::1)
    }

    "delete an existing key over an existing time" in {
      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val table = setup (random, scheduler)
      table.deleteCells (Kiwi##14)
      table.check (Kiwi##21::3, Kiwi##14, Kiwi##7::1)
    }}

  private def aCheckpointedTable (setup: (Random, StubScheduler) => SynthTable) {
    "handle a checkpoint" in {
      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val table = setup (random, scheduler)
      table.checkpoint() .expectPass()
      assert (table.primary.isEmpty)
      assert (table.secondary.isEmpty)
      assert (table.tiers.size > 0)
      table.check (Kiwi##21::3, Kiwi##14::2, Kiwi##7::1)
    }}

  private def aCheckpointingTable (
      setup: (Random, StubScheduler) => (StubDiskDrive, SynthTable, CallbackCaptor [Checkpoint])) {

    "finish the checkpoint" in {
      implicit val random = new Random (0)
      implicit val scheduler = StubScheduler.random (random)
      val (disk, table, cb) = setup (random, scheduler)
      disk.last.pass (())
      scheduler.run()
      cb.assertPassed()
      assert (table.secondary.isEmpty)
      assert (table.tiers.size > 0)
      table.check (Kiwi##21::3, Kiwi##14::2, Kiwi##7::1)
    }}

  "When a SynthTable has" - {

    "only empty tiers, it should" - {

      def setup () (implicit random: Random, scheduler: StubScheduler): SynthTable = {
        val disk = new StubDiskDrive
        val table = mkTable (disk)
        assert (table.primary.isEmpty)
        assert (table.secondary.isEmpty)
        assert (table.tiers.size == 0)
        table
      }

      "iterate no values" in {
        implicit val random = new Random (0)
        implicit val scheduler = StubScheduler.random (random)
        val table = setup()
        table.check ()
      }

      "handle a checkpoint" in {
        implicit val random = new Random (0)
        implicit val scheduler = StubScheduler.random (random)
        val table = setup()
        table.checkpoint() .expectPass()
        assert (table.primary.isEmpty)
        assert (table.secondary.isEmpty)
        assert (table.tiers.size == 0)
        table.check ()
      }

      "handle a put" in {
        implicit val random = new Random (0)
        implicit val scheduler = StubScheduler.random (random)
        val table = setup()
        table.putCells (Kiwi##7::1)
        assert (!table.primary.isEmpty)
        assert (table.secondary.isEmpty)
        assert (table.tiers.size == 0)
        table.check (Kiwi##7::1)
      }

      "handle a delete" in {
        implicit val random = new Random (0)
        implicit val scheduler = StubScheduler.random (random)
        val table = setup()
        table.deleteCells (Kiwi##1)
        assert (!table.primary.isEmpty)
        assert (table.secondary.isEmpty)
        assert (table.tiers.size == 0)
        table.check (Kiwi##1)
      }}

    "a non-empty primary tier, it should" - {

      def setup () (implicit random: Random, scheduler: StubScheduler): SynthTable = {
        val disk = new StubDiskDrive
        val table = mkTable (disk)
        table.putCells (Kiwi##7::1, Kiwi##14::2, Kiwi##21::3)
        assert (!table.primary.isEmpty)
        assert (table.secondary.isEmpty)
        assert (table.tiers.size == 0)
        table
      }

      behave like aCheckpointedTable (setup () (_, _))

      behave like aNonEmptyTable  (setup () (_, _))
    }

    "a non-empty secondary tier, it should" - {

      def setup () (implicit random: Random, scheduler: StubScheduler) = {
        val disk = new StubDiskDrive
        val table = mkTable (disk)
        table.putCells (Kiwi##7::1, Kiwi##14::2, Kiwi##21::3)
        disk.stop = true
        val cb = table .checkpoint() .capture()
        scheduler.run()
        cb.assertNotInvoked()
        assert (table.primary.isEmpty)
        assert (!table.secondary.isEmpty)
        assert (table.tiers.size == 0)
        disk.stop = false
        (disk, table, cb)
      }

      behave like aCheckpointingTable (setup () (_, _))

      behave like aNonEmptyTable ((r, s) => setup () (r, s) ._2)
    }

    "a non-empty primary and secondary tier, it should" - {

      def setup () (implicit random: Random, scheduler: StubScheduler) = {
        val disk = new StubDiskDrive
        val table = mkTable (disk)
        table.putCells (Kiwi##7::1, Kiwi##14::2)
        disk.stop = true
        val cb = table.checkpoint() .capture()
        scheduler.run()
        cb.assertNotInvoked()
        disk.stop = false
        table.putCells (Kiwi##21::3)
        assert (!table.primary.isEmpty)
        assert (!table.secondary.isEmpty)
        assert (table.tiers.size == 0)
        (disk, table, cb)
      }

      behave like aCheckpointingTable (setup () (_, _))

      behave like aNonEmptyTable ((r, s) => setup () (r, s) ._2)
    }

    "non-empty tertiary tiers, it should" - {

      def setup () (implicit random: Random, scheduler: StubScheduler): SynthTable = {
        val disk = new StubDiskDrive
        val table = mkTable (disk)
        table.putCells (Kiwi##7::1, Kiwi##14::2, Kiwi##21::3)
        table.checkpoint() .expectPass()
        assert (table.primary.isEmpty)
        assert (table.secondary.isEmpty)
        assert (table.tiers.size > 0)
        table
      }

      behave like aCheckpointedTable (setup () (_, _))

      behave like aNonEmptyTable (setup () (_, _))
    }

    "non-empty primary and tertiary tiers, it should" - {

      def setup () (implicit random: Random, scheduler: StubScheduler): SynthTable = {
        val disk = new StubDiskDrive
        val table = mkTable (disk)
        table.putCells (Kiwi##7::1, Kiwi##14::2)
        table.checkpoint() .expectPass()
        table.putCells (Kiwi##21::3)
        assert (!table.primary.isEmpty)
        assert (table.secondary.isEmpty)
        assert (table.tiers.size > 0)
        table
      }

      behave like aCheckpointedTable (setup () (_, _))

      behave like aNonEmptyTable (setup () (_, _))
    }}}
