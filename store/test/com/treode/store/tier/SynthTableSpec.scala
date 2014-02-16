package com.treode.store.tier

import java.nio.file.Paths

import com.treode.async.{CallbackCaptor, StubScheduler}
import com.treode.async.io.{File, StubFile}
import com.treode.disk.{Disks, DisksConfig, DiskGeometry}
import com.treode.store.StoreConfig
import com.treode.pickle.Picklers
import org.scalatest.FreeSpec

import TierTestTools._

class SynthTableSpec extends FreeSpec {

  private def mkTable (disk: File) (
      implicit scheduler: StubScheduler): SynthTable [String, Int] = {

    implicit val disksConfig = DisksConfig (14, 1<<24, 1<<16, 10, 1)
    implicit val recovery = Disks.recover()
    val disksCb = CallbackCaptor [Disks]
    val geometry = DiskGeometry (16, 12, 1<<30)
    recovery.attach (Seq ((Paths.get ("a"), disk, geometry)), disksCb)
    scheduler.runTasks()
    implicit val disks = disksCb.passed
    implicit val storeConfig = StoreConfig (1<<12)
    SynthTable (TierDescriptor (0x62C8FE56, Picklers.string, Picklers.int))
  }

  "When a SynthTable has only empty tiers" - {

    def setup () (implicit scheduler: StubScheduler): SynthTable [String, Int] = {
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
      table.checkpointAndPass()
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
      table.expectValue (1, 2)
    }

    "it should handle a delete" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.deleteAll (1)
      assert (!table.primary.isEmpty)
      assert (table.secondary.isEmpty)
      assert (table.tiers.isEmpty)
      table.expectValues ()
    }}

  "When a SynthTable has a non-empty primary tier" - {

    def setup () (implicit scheduler: StubScheduler): SynthTable [String, Int] = {
      val disk = new StubFile
      val table = mkTable (disk)
      table.putAll (1 -> 2, 2 -> 4, 3 -> 6)
      assert (!table.primary.isEmpty)
      assert (table.secondary.isEmpty)
      assert (table.tiers.isEmpty)
      table
    }

    "its iterator and get should yield those values" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.expectValues (1 -> 2, 2 -> 4, 3 -> 6)
      table.expectValue (1, 2)
      table.expectValue (2, 4)
      table.expectValue (3, 6)
    }

    "it should handle a checkpoint" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.checkpointAndPass()
      assert (table.primary.isEmpty)
      assert (table.secondary.isEmpty)
      assert (!table.tiers.isEmpty)
      table.expectValues (1 -> 2, 2 -> 4, 3 -> 6)
      table.expectValue (1, 2)
      table.expectValue (2, 4)
      table.expectValue (3, 6)
    }

    "it should handle a put of a new key" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.putAll (4 -> 8)
      table.expectValues (1 -> 2, 2 -> 4, 3 -> 6, 4 -> 8)
      table.expectValue (4, 8)
    }

    "it should handle a put of an existing key" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.putAll (2 -> 14)
      table.expectValues (1 -> 2, 2 -> 14, 3 -> 6)
      table.expectValue (2, 14)
    }

    "it should handle a delete of a new key" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.deleteAll (4)
      table.expectValues (1 -> 2, 2 -> 4, 3 -> 6)
      table.expectNone (4)
    }

    "it should handle a delete of an existing key" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.deleteAll (2)
      table.expectValues (1 -> 2, 3 -> 6)
      table.expectNone (2)
    }}

  "When a SynthTable has a non-empty secondary tier" - {

    def setup () (implicit scheduler: StubScheduler) = {
      val disk = new StubFile
      val table = mkTable (disk)
      table.putAll (1 -> 2, 2 -> 4, 3 -> 6)
      disk.stop = true
      val cb = CallbackCaptor [TierTable.Meta]
      table.checkpoint (cb)
      scheduler.runTasks()
      cb.expectNotInvoked()
      assert (table.primary.isEmpty)
      assert (!table.secondary.isEmpty)
      assert (table.tiers.isEmpty)
      disk.stop = false
      (disk, table, cb)
    }

    "its iterator and get should yield those values" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table, cb) = setup()
      table.expectValues (1 -> 2, 2 -> 4, 3 -> 6)
      table.expectValue (1, 2)
      table.expectValue (2, 4)
      table.expectValue (3, 6)
    }

    "it should finish the checkpoint" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table, cb) = setup()
      disk.last()
      scheduler.runTasks()
      cb.passed
      assert (table.primary.isEmpty)
      assert (table.secondary.isEmpty)
      assert (!table.tiers.isEmpty)
      table.expectValues (1 -> 2, 2 -> 4, 3 -> 6)
      table.expectValue (1, 2)
      table.expectValue (2, 4)
      table.expectValue (3, 6)
    }

    "it should handle a put of a new key" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table, cb) = setup()
      table.putAll (4 -> 8)
      table.expectValues (1 -> 2, 2 -> 4, 3 -> 6, 4 -> 8)
      table.expectValue (4, 8)
    }

    "it should handle a put of an existing key" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table, cb) = setup()
      table.putAll (2 -> 14)
      table.expectValues (1 -> 2, 2 -> 14, 3 -> 6)
      table.expectValue (2, 14)
    }

    "it should handle a delete of a new key" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table, cb) = setup()
      table.deleteAll (4)
      table.expectValues (1 -> 2, 2 -> 4, 3 -> 6)
      table.expectNone (4)
    }

    "it should handle a delete of an existing key" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table, cb) = setup()
      table.deleteAll (2)
      table.expectValues (1 -> 2, 3 -> 6)
      table.expectNone (2)
    }}

  "When a SynthTable has a non-empty primary and secondary tier" - {

    def setup () (implicit scheduler: StubScheduler) = {
      val disk = new StubFile
      val table = mkTable (disk)
      table.putAll (1 -> 2, 2 -> 4, 3 -> 6)
      disk.stop = true
      val cb = CallbackCaptor [TierTable.Meta]
      table.checkpoint (cb)
      scheduler.runTasks()
      cb.expectNotInvoked()
      disk.stop = false
      table.putAll (2 -> 14, 4 -> 18)
      assert (!table.primary.isEmpty)
      assert (!table.secondary.isEmpty)
      assert (table.tiers.isEmpty)
      (disk, table, cb)
    }

    "its iterator and get should yield those values" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table, cb) = setup()
      table.expectValues (1 -> 2, 2 -> 14, 3 -> 6, 4 -> 18)
      table.expectValue (1, 2)
      table.expectValue (2, 14)
      table.expectValue (3, 6)
      table.expectValue (4, 18)
    }

    "it should finish the checkpoint" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table, cb) = setup()
      disk.last()
      scheduler.runTasks()
      cb.passed
      assert (!table.primary.isEmpty)
      assert (table.secondary.isEmpty)
      assert (!table.tiers.isEmpty)
      table.expectValues (1 -> 2, 2 -> 14, 3 -> 6, 4 -> 18)
      table.expectValue (1, 2)
      table.expectValue (2, 14)
      table.expectValue (3, 6)
      table.expectValue (4, 18)
    }

    "it should handle a put of a new key" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table, cb) = setup()
      table.putAll (5 -> 30)
      table.expectValues (1 -> 2, 2 -> 14, 3 -> 6, 4 -> 18, 5 -> 30)
      table.expectValue (5, 30)
    }

    "it should handle a put of an existing key" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table, cb) = setup()
      table.putAll (3 -> 26)
      table.expectValues (1 -> 2, 2 -> 14, 3 -> 26, 4 -> 18)
      table.expectValue (3, 26)
    }

    "it should handle a delete of a new key" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table, cb) = setup()
      table.deleteAll (5)
      table.expectValues (1 -> 2, 2 -> 14, 3 -> 6, 4 -> 18)
      table.expectNone (5)
    }

    "it should handle a delete of an existing key" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table, cb) = setup()
      table.deleteAll (3)
      table.expectValues (1 -> 2, 2 -> 14, 4 -> 18)
      table.expectNone (3)
    }}

  "When a SynthTable has a non-empty tertiary tiers" - {

    def setup () (implicit scheduler: StubScheduler): SynthTable [String, Int] = {
      val disk = new StubFile
      val table = mkTable (disk)
      table.putAll (1 -> 2, 2 -> 4, 3 -> 6)
      table.checkpointAndPass()
      assert (table.primary.isEmpty)
      assert (table.secondary.isEmpty)
      assert (!table.tiers.isEmpty)
      table
    }

    "its iterator and get should yield those values" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.expectValues (1 -> 2, 2 -> 4, 3 -> 6)
      table.expectValue (1, 2)
      table.expectValue (2, 4)
      table.expectValue (3, 6)
    }

    "it should handle a checkpoint" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.checkpointAndPass()
      assert (table.primary.isEmpty)
      assert (table.secondary.isEmpty)
      assert (!table.tiers.isEmpty)
      table.expectValues (1 -> 2, 2 -> 4, 3 -> 6)
      table.expectValue (1, 2)
      table.expectValue (2, 4)
      table.expectValue (3, 6)
    }

    "it should handle a put of a new key" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.putAll (4 -> 8)
      table.expectValues (1 -> 2, 2 -> 4, 3 -> 6, 4 -> 8)
      table.expectValue (4, 8)
    }

    "it should handle a put of an existing key" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.putAll (2 -> 14)
      table.expectValues (1 -> 2, 2 -> 14, 3 -> 6)
      table.expectValue (2, 14)
    }

    "it should handle a delete of a new key" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.deleteAll (4)
      table.expectValues (1 -> 2, 2 -> 4, 3 -> 6)
      table.expectNone (4)
    }

    "it should handle a delete of an existing key" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.deleteAll (2)
      table.expectValues (1 -> 2, 3 -> 6)
      table.expectNone (2)
    }}

  "When a SynthTable has a non-empty primary and tertiary tiers" - {

    def setup () (implicit scheduler: StubScheduler): SynthTable [String, Int] = {
      val disk = new StubFile
      val table = mkTable (disk)
      table.putAll (1 -> 2, 2 -> 4, 3 -> 6)
      table.checkpointAndPass()
      table.putAll (2 -> 14, 4 -> 18)
      assert (!table.primary.isEmpty)
      assert (table.secondary.isEmpty)
      assert (!table.tiers.isEmpty)
      table
    }

    "its iterator and get should yield those values" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.expectValues (1 -> 2, 2 -> 14, 3 -> 6, 4 -> 18)
      table.expectValue (1, 2)
      table.expectValue (2, 14)
      table.expectValue (3, 6)
      table.expectValue (4, 18)
    }

    "it should handle a checkpoint" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.checkpointAndPass()
      assert (table.primary.isEmpty)
      assert (table.secondary.isEmpty)
      assert (!table.tiers.isEmpty)
      table.expectValues (1 -> 2, 2 -> 14, 3 -> 6, 4 -> 18)
      table.expectValue (1, 2)
      table.expectValue (2, 14)
      table.expectValue (3, 6)
      table.expectValue (4, 18)
    }}}
