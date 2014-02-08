package systest

import java.nio.file.Paths

import com.treode.async.{CallbackCaptor, StubScheduler}
import com.treode.async.io.{File, StubFile}
import com.treode.disk.{Disks, DiskDriveConfig}
import org.scalatest.FreeSpec

import SystestTools._

class SynthTableSpec extends FreeSpec {

  def mkTable (disk: File) (implicit scheduler: StubScheduler): SynthTable = {
    implicit val recovery = Disks.recover()
    val disksCb = new CallbackCaptor [Disks]
    val diskDriveConfig = DiskDriveConfig (16, 12, 1<<30)
    recovery.attach (Seq ((Paths.get ("a"), disk, diskDriveConfig)), disksCb)
    scheduler.runTasks()
    implicit val disks = disksCb.passed
    implicit val testConfig = new TestConfig (1<<12)
    SynthTable()
  }

  "When a SynthTable has only empty tiers" - {

    def setup () (implicit scheduler: StubScheduler): SynthTable = {
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
      table.putAndPass (1, 2)
      assert (!table.primary.isEmpty)
      assert (table.secondary.isEmpty)
      assert (table.tiers.isEmpty)
      table.expectValues (1 -> 2)
      table.expectValue (1, 2)
    }

    "it should handle a delete" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.deleteAndPass (1)
      assert (!table.primary.isEmpty)
      assert (table.secondary.isEmpty)
      assert (table.tiers.isEmpty)
      table.expectValues ()
    }}

  "When a SynthTable has a non-empty primary tier" - {

    def setup () (implicit scheduler: StubScheduler): SynthTable = {
      val disk = new StubFile
      val table = mkTable (disk)
      table.putAndPass (1 -> 2, 2 -> 4, 3 -> 6)
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
      table.putAndPass (4, 8)
      table.expectValues (1 -> 2, 2 -> 4, 3 -> 6, 4 -> 8)
      table.expectValue (4, 8)
    }

    "it should handle a put of an existing key" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.putAndPass (2 -> 14)
      table.expectValues (1 -> 2, 2 -> 14, 3 -> 6)
      table.expectValue (2, 14)
    }

    "it should handle a delete of a new key" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.deleteAndPass (4)
      table.expectValues (1 -> 2, 2 -> 4, 3 -> 6)
      table.expectNone (4)
    }

    "it should handle a delete of an existing key" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.deleteAndPass (2)
      table.expectValues (1 -> 2, 3 -> 6)
      table.expectNone (2)
    }}

  "When a SynthTable has a non-empty secondary tier" - {

    def setup () (implicit scheduler: StubScheduler): (StubFile, SynthTable) = {
      val disk = new StubFile
      val table = mkTable (disk)
      table.putAndPass (1 -> 2, 2 -> 4, 3 -> 6)
      disk.stop = true
      val cb = new CallbackCaptor [Tiers]
      table.checkpoint (cb)
      scheduler.runTasks()
      assert (!cb.wasInvoked)
      assert (table.primary.isEmpty)
      assert (!table.secondary.isEmpty)
      assert (table.tiers.isEmpty)
      disk.stop = false
      (disk, table)
    }

    "its iterator and get should yield those values" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table) = setup()
      table.expectValues (1 -> 2, 2 -> 4, 3 -> 6)
      table.expectValue (1, 2)
      table.expectValue (2, 4)
      table.expectValue (3, 6)
    }

    "it should finish the checkpoint" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table) = setup()
      disk.last()
      scheduler.runTasks()
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
      val (disk, table) = setup()
      table.putAndPass (4, 8)
      table.expectValues (1 -> 2, 2 -> 4, 3 -> 6, 4 -> 8)
      table.expectValue (4, 8)
    }

    "it should handle a put of an existing key" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table) = setup()
      table.putAndPass (2 -> 14)
      table.expectValues (1 -> 2, 2 -> 14, 3 -> 6)
      table.expectValue (2, 14)
    }

    "it should handle a delete of a new key" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table) = setup()
      table.deleteAndPass (4)
      table.expectValues (1 -> 2, 2 -> 4, 3 -> 6)
      table.expectNone (4)
    }

    "it should handle a delete of an existing key" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table) = setup()
      table.deleteAndPass (2)
      table.expectValues (1 -> 2, 3 -> 6)
      table.expectNone (2)
    }}

  "When a SynthTable has a non-empty primary and secondary tier" - {

    def setup () (implicit scheduler: StubScheduler): (StubFile, SynthTable) = {
      val disk = new StubFile
      val table = mkTable (disk)
      table.putAndPass (1 -> 2, 2 -> 4, 3 -> 6)
      disk.stop = true
      val cb = new CallbackCaptor [Tiers]
      table.checkpoint (cb)
      scheduler.runTasks()
      assert (!cb.wasInvoked)
      disk.stop = false
      table.putAndPass (2 -> 14, 4 -> 18)
      assert (!table.primary.isEmpty)
      assert (!table.secondary.isEmpty)
      assert (table.tiers.isEmpty)
      (disk, table)
    }

    "its iterator and get should yield those values" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table) = setup()
      table.expectValues (1 -> 2, 2 -> 14, 3 -> 6, 4 -> 18)
      table.expectValue (1, 2)
      table.expectValue (2, 14)
      table.expectValue (3, 6)
      table.expectValue (4, 18)
    }

    "it should finish the checkpoint" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table) = setup()
      disk.last()
      scheduler.runTasks()
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
      val (disk, table) = setup()
      table.putAndPass (5, 30)
      table.expectValues (1 -> 2, 2 -> 14, 3 -> 6, 4 -> 18, 5 -> 30)
      table.expectValue (5, 30)
    }

    "it should handle a put of an existing key" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table) = setup()
      table.putAndPass (3 -> 26)
      table.expectValues (1 -> 2, 2 -> 14, 3 -> 26, 4 -> 18)
      table.expectValue (3, 26)
    }

    "it should handle a delete of a new key" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table) = setup()
      table.deleteAndPass (5)
      table.expectValues (1 -> 2, 2 -> 14, 3 -> 6, 4 -> 18)
      table.expectNone (5)
    }

    "it should handle a delete of an existing key" in {
      implicit val scheduler = StubScheduler.random()
      val (disk, table) = setup()
      table.deleteAndPass (3)
      table.expectValues (1 -> 2, 2 -> 14, 4 -> 18)
      table.expectNone (3)
    }}

  "When a SynthTable has a non-empty tertiary tiers" - {

    def setup () (implicit scheduler: StubScheduler): SynthTable = {
      val disk = new StubFile
      val table = mkTable (disk)
      table.putAndPass (1 -> 2, 2 -> 4, 3 -> 6)
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
      table.putAndPass (4, 8)
      table.expectValues (1 -> 2, 2 -> 4, 3 -> 6, 4 -> 8)
      table.expectValue (4, 8)
    }

    "it should handle a put of an existing key" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.putAndPass (2 -> 14)
      table.expectValues (1 -> 2, 2 -> 14, 3 -> 6)
      table.expectValue (2, 14)
    }

    "it should handle a delete of a new key" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.deleteAndPass (4)
      table.expectValues (1 -> 2, 2 -> 4, 3 -> 6)
      table.expectNone (4)
    }

    "it should handle a delete of an existing key" in {
      implicit val scheduler = StubScheduler.random()
      val table = setup()
      table.deleteAndPass (2)
      table.expectValues (1 -> 2, 3 -> 6)
      table.expectNone (2)
    }}

  "When a SynthTable has a non-empty primary and tertiary tiers" - {

    def setup () (implicit scheduler: StubScheduler): SynthTable = {
      val disk = new StubFile
      val table = mkTable (disk)
      table.putAndPass (1 -> 2, 2 -> 4, 3 -> 6)
      table.checkpointAndPass()
      table.putAndPass (2 -> 14, 4 -> 18)
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
