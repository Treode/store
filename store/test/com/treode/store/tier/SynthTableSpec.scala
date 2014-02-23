package com.treode.store.tier

import java.nio.file.Paths

import com.treode.async.{CallbackCaptor, StubScheduler}
import com.treode.async.io.{File, StubFile}
import com.treode.disk.{Disks, DisksConfig, DiskGeometry}
import com.treode.store.StoreConfig
import com.treode.pickle.Picklers
import org.scalatest.FreeSpec

import TierTable.Meta
import TierTestTools._

class SynthTableSpec extends FreeSpec {

  private def mkTable (disk: File) (
      implicit scheduler: StubScheduler): SynthTable [Int, Int] = {

    implicit val disksConfig = DisksConfig (14, 1<<24, 1<<16, 10, 1)
    implicit val recovery = Disks.recover()
    val geometry = DiskGeometry (16, 12, 1<<30)
    implicit val launch = recovery.attach (Seq ((Paths.get ("a"), disk, geometry))) .pass
    implicit val disks = launch.disks
    launch.launch()
    implicit val storeConfig = StoreConfig (4, 1<<12)
    SynthTable (TierDescriptor (0x62C8FE56, Picklers.int, Picklers.int))
  }

  private def aNonEmptyTable (setup: StubScheduler => SynthTable [_, _]) {

    "its iterator and get should yield those values" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup (scheduler)
      table.expectValues (1 -> 2, 3 -> 6, 5 -> 10)
      table.expectCeiling (0, 1, 1, 2)
      table.expectCeiling (1, 2, 1, 2)
      table.expectValue (1, 2)
      table.expectCeiling (2, 3, 3, 6)
      table.expectCeiling (3, 4, 3, 6)
      table.expectValue (3, 6)
      table.expectCeiling (4, 5, 5, 10)
      table.expectCeiling (5, 6, 5, 10)
      table.expectValue (5, 10)
      table.expectNoCeiling (6, 7, 7)
    }

    "it should handle a put of a new key" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup (scheduler)
      table.putAll (7 -> 14)
      table.expectValues (1 -> 2, 3 -> 6, 5 -> 10, 7 -> 14)
      table.expectCeiling (0, 1, 1, 2)
      table.expectCeiling (1, 2, 1, 2)
      table.expectValue (1, 2)
      table.expectCeiling (2, 3, 3, 6)
      table.expectCeiling (3, 4, 3, 6)
      table.expectValue (3, 6)
      table.expectCeiling (4, 5, 5, 10)
      table.expectCeiling (5, 6, 5, 10)
      table.expectValue (5, 10)
      table.expectCeiling (6, 7, 7, 14)
      table.expectCeiling (7, 8, 7, 14)
      table.expectValue (7, 14)
      table.expectNoCeiling (8, 9, 9)
    }

    "it should handle a put of an existing key" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup (scheduler)
      table.putAll (3 -> 16)
      table.expectValues (1 -> 2, 3 -> 16, 5 -> 10)
      table.expectCeiling (0, 1, 1, 2)
      table.expectCeiling (1, 2, 1, 2)
      table.expectValue (1, 2)
      table.expectCeiling (2, 3, 3, 16)
      table.expectCeiling (3, 4, 3, 16)
      table.expectValue (3, 16)
      table.expectCeiling (4, 5, 5, 10)
      table.expectCeiling (5, 6, 5, 10)
      table.expectValue (5, 10)
      table.expectNoCeiling (6, 7, 7)
    }

    "it should handle a delete of a new key" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup (scheduler)
      table.deleteAll (7)
      table.expectValues (1 -> 2, 3 -> 6, 5 -> 10)
      table.expectCeiling (0, 1, 1, 2)
      table.expectCeiling (1, 2, 1, 2)
      table.expectValue (1, 2)
      table.expectCeiling (2, 3, 3, 6)
      table.expectCeiling (3, 4, 3, 6)
      table.expectValue (3, 6)
      table.expectCeiling (4, 5, 5, 10)
      table.expectCeiling (5, 6, 5, 10)
      table.expectValue (5, 10)
      table.expectNoCeiling (6, 7, 7)
      table.expectNoCeiling (7, 8, 7)
    }

    "it should handle a delete of an existing key" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup (scheduler)
      table.deleteAll (3)
      table.expectValues (1 -> 2, 5 -> 10)
      table.expectCeiling (0, 1, 1, 2)
      table.expectCeiling (1, 2, 1, 2)
      table.expectValue (1, 2)
      table.expectNoCeiling (2, 3, 3)
      table.expectNoCeiling (3, 4, 3)
      table.expectNone (3)
      table.expectCeiling (4, 5, 5, 10)
      table.expectCeiling (5, 6, 5, 10)
      table.expectValue (5, 10)
      table.expectNoCeiling (6, 7, 7)
    }}

  private def aCheckpointedTable (setup: StubScheduler => SynthTable [_, _]) {
    "it should handle a checkpoint" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup (scheduler)
      table.checkpoint() .pass
      assert (table.primary.isEmpty)
      assert (table.secondary.isEmpty)
      assert (!table.tiers.isEmpty)
      table.expectValues (1 -> 2, 3 -> 6, 5 -> 10)
      table.expectCeiling (0, 1, 1, 2)
      table.expectCeiling (1, 2, 1, 2)
      table.expectValue (1, 2)
      table.expectCeiling (2, 3, 3, 6)
      table.expectCeiling (3, 4, 3, 6)
      table.expectValue (3, 6)
      table.expectCeiling (4, 5, 5, 10)
      table.expectCeiling (5, 6, 5, 10)
      table.expectValue (5, 10)
      table.expectNoCeiling (6, 7, 7)
    }}

  private def aCheckpointingTable (
      setup: StubScheduler => (StubFile, SynthTable [Int, Int], CallbackCaptor [Meta])) {

    "it should finish the checkpoint" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table, cb) = setup (scheduler)
      disk.last.pass()
      scheduler.runTasks()
      cb.passed
      assert (table.secondary.isEmpty)
      assert (!table.tiers.isEmpty)
      table.expectValues (1 -> 2, 3 -> 6, 5 -> 10)
      table.expectCeiling (0, 1, 1, 2)
      table.expectCeiling (1, 2, 1, 2)
      table.expectValue (1, 2)
      table.expectCeiling (2, 3, 3, 6)
      table.expectCeiling (3, 4, 3, 6)
      table.expectValue (3, 6)
      table.expectCeiling (4, 5, 5, 10)
      table.expectCeiling (5, 6, 5, 10)
      table.expectValue (5, 10)
      table.expectNoCeiling (6, 7, 7)
    }}

  "When a SynthTable has only empty tiers" - {

    def setup () (implicit scheduler: StubScheduler): SynthTable [Int, Int] = {
      val disk = new StubFile
      val table = mkTable (disk)
      assert (table.primary.isEmpty)
      assert (table.secondary.isEmpty)
      assert (table.tiers.isEmpty)
      table
    }

    "its iterator should yield no values" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.expectValues ()
    }

    "it should handle a checkpoint" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.checkpoint() .pass
      assert (table.primary.isEmpty)
      assert (table.secondary.isEmpty)
      assert (!table.tiers.isEmpty)
      table.expectValues ()
    }

    "it should handle a put" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.putAll (1 -> 2)
      assert (!table.primary.isEmpty)
      assert (table.secondary.isEmpty)
      assert (table.tiers.isEmpty)
      table.expectValues (1 -> 2)
      table.expectCeiling (0, 1, 1, 2)
      table.expectCeiling (1, 2, 1, 2)
      table.expectValue (1, 2)
      table.expectNoCeiling (2, 3, 3)
    }

    "it should handle a delete" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.deleteAll (1)
      assert (!table.primary.isEmpty)
      assert (table.secondary.isEmpty)
      assert (table.tiers.isEmpty)
      table.expectValues ()
      table.expectNoCeiling (0, 1, 1)
      table.expectNone (1)
      table.expectNoCeiling (1, 2, 1)
      table.expectNoCeiling (2, 3, 3)
    }}

  "When a SynthTable has a non-empty primary tier" - {

    def setup () (implicit scheduler: StubScheduler): SynthTable [Int, Int] = {
      val disk = new StubFile
      val table = mkTable (disk)
      table.putAll (1 -> 2, 3 -> 6, 5 -> 10)
      assert (!table.primary.isEmpty)
      assert (table.secondary.isEmpty)
      assert (table.tiers.isEmpty)
      table
    }

    behave like aCheckpointedTable (s => setup () (s))

    behave like aNonEmptyTable (s => setup () (s))
  }

  "When a SynthTable has a non-empty secondary tier" - {

    def setup () (implicit scheduler: StubScheduler) = {
      val disk = new StubFile
      val table = mkTable (disk)
      table.putAll (1 -> 2, 3 -> 6, 5 -> 10)
      disk.stop = true
      val cb = table.checkpoint() .capture()
      scheduler.runTasks()
      cb.expectNotInvoked()
      assert (table.primary.isEmpty)
      assert (!table.secondary.isEmpty)
      assert (table.tiers.isEmpty)
      disk.stop = false
      (disk, table, cb)
    }

    behave like aCheckpointingTable (s => setup () (s))

    behave like aNonEmptyTable (s => setup () (s) ._2)
  }

  "When a SynthTable has a non-empty primary and secondary tier" - {

    def setup () (implicit scheduler: StubScheduler) = {
      val disk = new StubFile
      val table = mkTable (disk)
      table.putAll (1 -> 2, 3 -> 6)
      disk.stop = true
      val cb = table.checkpoint() .capture()
      scheduler.runTasks()
      cb.expectNotInvoked()
      disk.stop = false
      table.putAll (5 -> 10)
      assert (!table.primary.isEmpty)
      assert (!table.secondary.isEmpty)
      assert (table.tiers.isEmpty)
      (disk, table, cb)
    }

    behave like aCheckpointingTable (s => setup () (s))

    behave like aNonEmptyTable (s => setup () (s) ._2)
  }

  "When a SynthTable has non-empty tertiary tiers" - {

    def setup () (implicit scheduler: StubScheduler): SynthTable [Int, Int] = {
      val disk = new StubFile
      val table = mkTable (disk)
      table.putAll (1 -> 2, 3 -> 6, 5 -> 10)
      table.checkpoint() .pass
      assert (table.primary.isEmpty)
      assert (table.secondary.isEmpty)
      assert (!table.tiers.isEmpty)
      table
    }

    behave like aCheckpointedTable (s => setup () (s))

    behave like aNonEmptyTable (s => setup () (s))
  }

  "When a SynthTable has non-empty primary and tertiary tiers" - {

    def setup () (implicit scheduler: StubScheduler): SynthTable [Int, Int] = {
      val disk = new StubFile
      val table = mkTable (disk)
      table.putAll (1 -> 2, 3 -> 6)
      table.checkpoint() .pass
      table.putAll (5 -> 10)
      assert (!table.primary.isEmpty)
      assert (table.secondary.isEmpty)
      assert (!table.tiers.isEmpty)
      table
    }

    behave like aCheckpointedTable (s => setup () (s))

    behave like aNonEmptyTable (s => setup () (s))
  }}
