package com.treode.disk

import java.util.ArrayList
import scala.collection.JavaConversions
import scala.util.Random

import com.treode.async.{Async, AsyncConversions, StubScheduler}
import com.treode.async.io.StubFile
import com.treode.tags.{Intensive, Periodic}
import org.scalacheck.Gen
import org.scalatest.{Assertions, FreeSpec}
import org.scalatest.prop.PropertyChecks
import org.scalatest.time.SpanSugar

import Async.{guard, latch, supply}
import AsyncConversions._
import DiskTestTools._
import DiskSystemSpec._
import JavaConversions._
import PropertyChecks._
import SpanSugar._

class DiskSystemSpec extends FreeSpec with CrashChecks {

  override val timeLimit = 15 minutes

  val seeds = Gen.choose (0L, Long.MaxValue)

  "The logger should replay items" - {

    "without checkpoints using one disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DisksConfig (0, 8, 1<<30, 1<<30, 1<<30, 1)
        val geometry = DiskGeometry (10, 6, 1<<20)
        val disk = new StubFile (size = 1<<20) (null)
        val tracker = new LogTracker

        setup { implicit scheduler =>
          implicit val recovery = Disks.recover()
          val launch = recovery.attachAndWait (("a", disk, geometry)) .pass
          import launch.disks
          launch.checkpoint (fail ("Expected no checkpoints"))
          launch.launch()
          tracker.batches (80, 40, 10)
        }

        .recover { implicit scheduler =>
          implicit val recovery = Disks.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val disks = recovery.reattachAndLaunch (("a", disk))
          replayer.check (tracker)
        }}}

    "using one disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DisksConfig (0, 8, 1<<30, 57, 1<<30, 1)
        val geometry = DiskGeometry (10, 6, 1<<20)
        val disk = new StubFile (size=1<<20) (null)
        val tracker = new LogTracker

        setup { implicit scheduler =>
          implicit val recovery = Disks.recover()
          implicit val launch = recovery.attachAndWait (("a", disk, geometry)) .pass
          import launch.disks
          var checkpoint = false
          tracker.attach (launch)
          launch.checkpoint (supply (checkpoint = true))
          launch.launch()
          for {
            _ <- tracker.batches (80, 40, 10)
          } yield {
            assert (checkpoint, "Expected a checkpoint")
          }}

        .recover { implicit scheduler =>
          implicit val recovery = Disks.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val disks = recovery.reattachAndLaunch (("a", disk))
          replayer.check (tracker)
        }}}

    "using multiple disks" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DisksConfig (0, 8, 1<<30, 57, 1<<30, 1)
        val geometry = DiskGeometry (10, 6, 1<<20)
        val disk1 = new StubFile (size = 1<<20) (null)
        val disk2 = new StubFile (size = 1<<20) (null)
        val disk3 = new StubFile (size = 1<<20) (null)
        val tracker = new LogTracker

        setup { implicit scheduler =>
          implicit val recovery = Disks.recover()
          val launch = recovery.attachAndWait (
              ("a", disk1, geometry),
              ("b", disk2, geometry),
              ("c", disk3, geometry)) .pass
          import launch.disks
          var checkpoint = false
          tracker.attach (launch)
          launch.checkpoint (supply (checkpoint = true))
          launch.launch()
          for {
            _ <- tracker.batches (80, 40, 10)
          } yield {
            assert (checkpoint, "Expected a checkpoint")
          }}

        .recover { implicit scheduler =>
          implicit val recovery = Disks.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val disks = recovery.reattachAndLaunch (
              ("a", disk1),
              ("b", disk2),
              ("c", disk3))
          replayer.check (tracker)
        }}}

    "while attaching a disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DisksConfig (0, 8, 1<<30, 17, 1<<30, 1)
        val geometry = DiskGeometry (10, 6, 1<<20)
        val disk1 = new StubFile (size=1<<20) (null)
        val disk2 = new StubFile (size=1<<20) (null)
        val tracker = new LogTracker

        setup { implicit scheduler =>
          implicit val recovery = Disks.recover()
          implicit val launch = recovery.attachAndWait (("a", disk1, geometry)) .pass
          import launch.{disks, controller}
          var checkpoint = false
          tracker.attach (launch)
          launch.checkpoint (supply (checkpoint = true))
          launch.launch()
          for {
            _ <- tracker.batches (80, 2, 10, 0)
            _ <- latch (
                tracker.batch (80, 2, 10),
                controller.attachAndWait (("b", disk2, geometry)))
            _ <- tracker.batches (80, 2, 10, 3)
          } yield {
            assert (checkpoint, "Expected a checkpoint")
          }}

        .recover { implicit scheduler =>
          implicit val recovery = Disks.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val disks = recovery.reopenAndLaunch ("a") (("a", disk1), ("b", disk2))
          replayer.check (tracker)
        }}}

    "while draining a disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DisksConfig (0, 8, 1<<30, 17, 1<<30, 1)
        val geometry = DiskGeometry (10, 6, 1<<20)
        val disk1 = new StubFile (size=1<<20) (null)
        val disk2 = new StubFile (size=1<<20) (null)
        val tracker = new LogTracker

        setup { implicit scheduler =>
          implicit val recovery = Disks.recover()
          implicit val launch =
            recovery.attachAndWait (("a", disk1, geometry), ("b", disk2, geometry)) .pass
          import launch.{disks, controller}
          var checkpoint = false
          tracker.attach (launch)
          launch.checkpoint (supply (checkpoint = true))
          launch.launch()
          for {
            _ <- tracker.batches (80, 2, 3, 0)
            _ <- latch (
                tracker.batch (80, 2, 3),
                controller.drainAndWait ("b"))
            _ <- tracker.batches (80, 2, 3, 3)
          } yield {
            assert (checkpoint, "Expected a checkpoint")
          }}

        .recover { implicit scheduler =>
          implicit val recovery = Disks.recover()
          val replayer = new LogReplayer
          replayer.attach (recovery)
          implicit val disks = recovery.reopenAndLaunch ("a") (("a", disk1), ("b", disk2))
          replayer.check (tracker)
        }}}}

  "The pager should read and write" - {

    "without cleaning using one disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DisksConfig (0, 8, 1<<30, 1<<30, 1<<30, 1)
        val geometry = DiskGeometry (10, 6, 1<<20)
        val disk = new StubFile (size=1<<20) (null)
        var tracker = new StuffTracker

        setup { implicit scheduler =>
          implicit val recovery = Disks.recover()
          implicit val disks = recovery.attachAndLaunch (("a", disk, geometry))
          tracker.batch (40, 10)
        }

        .recover { implicit scheduler =>
          implicit val recovery = Disks.recover()
          implicit val disks = recovery.reattachAndLaunch (("a", disk))
          tracker.check()
        }}}

    "using one disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DisksConfig (0, 8, 1<<30, 1<<30, 3, 1)
        val geometry = DiskGeometry (10, 6, 1<<20)
        val disk = new StubFile () (null)
        var tracker = new StuffTracker

        setup { implicit scheduler =>
          implicit val recovery = Disks.recover()
          implicit val launch = recovery.attachAndWait (("a", disk, geometry)) .pass
          import launch.disks
          tracker.attach (launch)
          launch.launch()

          for {
            _ <- tracker.batch (40, 10)
          } yield {
            assert (tracker.probed && tracker.compacted, "Expected cleaning.")
          }}

        .recover { implicit scheduler =>
          implicit val recovery = Disks.recover()
          implicit val disks = recovery.reattachAndLaunch (("a", disk))
          tracker.check()
        }}}

    "using multiple disks" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DisksConfig (0, 8, 1<<30, 1<<30, 3, 1)
        val geometry = DiskGeometry (10, 6, 1<<20)
        val disk1 = new StubFile () (null)
        val disk2 = new StubFile () (null)
        val disk3 = new StubFile () (null)
        var tracker = new StuffTracker

        setup { implicit scheduler =>
          implicit val recovery = Disks.recover()
          val launch = recovery.attachAndWait (
              ("a", disk1, geometry),
              ("b", disk2, geometry),
              ("c", disk3, geometry)) .pass
          import launch.disks
          tracker.attach (launch)
          launch.launch()

          for {
            _ <- tracker.batch (40, 10)
          } yield {
            assert (tracker.probed && tracker.compacted, "Expected cleaning.")
          }}

        .recover { implicit scheduler =>
          implicit val recovery = Disks.recover()
          implicit val disks = recovery.reattachAndLaunch (
              ("a", disk1),
              ("b", disk2),
              ("c", disk3))
          tracker.check()
        }}}

    "while attaching a disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DisksConfig (0, 8, 1<<30, 1<<30, 3, 1)
        val geometry = DiskGeometry (10, 6, 1<<20)
        val disk1 = new StubFile () (null)
        val disk2 = new StubFile () (null)
        var tracker = new StuffTracker

        setup { implicit scheduler =>
          implicit val recovery = Disks.recover()
          implicit val launch = recovery.attachAndWait (("a", disk1, geometry)) .pass
          import launch.{disks, controller}
          tracker.attach (launch)
          launch.launch()

          for {
            _ <- tracker.batch (7, 10)
            _ <- latch (
                tracker.batch (7, 10),
                controller.attachAndWait (("b", disk2, geometry)))
            _ <- tracker.batch (7, 10)
          } yield {
            assert (tracker.probed && tracker.compacted, "Expected cleaning.")
          }}

        .recover { implicit scheduler =>
          implicit val recovery = Disks.recover()
          implicit val disks = recovery.reopenAndLaunch ("a") (("a", disk1), ("b", disk2))
          tracker.check()
        }}}

    "while draining a disk" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DisksConfig (0, 8, 1<<30, 1<<30, 3, 1)
        val geometry = DiskGeometry (10, 6, 1<<20)
        val disk1 = new StubFile () (null)
        val disk2 = new StubFile () (null)
        var tracker = new StuffTracker

        setup { implicit scheduler =>
          implicit val recovery = Disks.recover()
          implicit val launch =
            recovery.attachAndWait (("a", disk1, geometry), ("b", disk2, geometry)) .pass
          import launch.{disks, controller}
          tracker.attach (launch)
          launch.launch()

          for {
            _ <- tracker.batch (7, 10)
            _ <- latch (
                tracker.batch (7, 10),
                controller.drainAndWait ("b"))
            _ <- tracker.batch (7, 10)
          } yield {
            assert (tracker.probed && tracker.compacted, "Expected cleaning.")
          }}

        .recover { implicit scheduler =>
          implicit val recovery = Disks.recover()
          implicit val disks = recovery.reopenAndLaunch ("a") (("a", disk1), ("b", disk2))
          tracker.check()
        }}}

    "more data than disk" taggedAs (Intensive, Periodic) in {
      forAll (seeds) { seed =>

        implicit val random = new Random (seed)
        implicit val config = DisksConfig (0, 8, 1<<30, 1<<30, 3, 1)
        val geometry = DiskGeometry (10, 6, 1<<20)
        val disk = new StubFile () (null)
        var tracker = new StuffTracker

        implicit val scheduler = StubScheduler.random (random)
        implicit val recovery = Disks.recover()
        implicit val launch = recovery.attachAndWait (("a", disk, geometry)) .pass
        import launch.disks
        tracker.attach (launch)
        launch.launch()
        tracker.batch (1000, 10) .pass
        assert (tracker.maximum < (2<<18), "Expected controlled growth.")
      }}}}

object DiskSystemSpec {
  import Assertions._

  object records {
    import DiskPicklers._

    val put =
      RecordDescriptor (0xBF, tuple (uint, uint, uint, uint))

    val checkpoint =
      RecordDescriptor (0x9B, tuple (uint, pos))
  }

  object pagers {
    import DiskPicklers._

    val table =
      PageDescriptor (0x79, uint, map (uint, uint))

    val stuff =
      PageDescriptor (0x26, fixedLong, Stuff.pickler)
  }

  class LogTracker {

    private var attempted = Map.empty [Int, Int] .withDefaultValue (-1)
    private var accepted = Map.empty [Int, Int] .withDefaultValue (-1)
    private var gen = 0

    def put (n: Int, k: Int, v: Int) (implicit disks: Disks): Async [Unit] = {
      attempted += k -> v
      val g = gen
      for {
        _ <- records.put.record (n, g, k, v)
      } yield {
        accepted += k -> v
      }}

    def batch (nkeys: Int, round: Int, nputs: Int) (
        implicit random: Random, disks: Disks): Async [Unit] =
      for (k <- random.nextInts (nputs, nkeys) .latch.unit)
        put (round, k, random.nextInt (1<<20))

    def batches (nkeys: Int, nbatches: Int, nputs: Int, start: Int = 0) (
        implicit random: Random, scheduler: StubScheduler, disks: Disks): Async [Unit] =
      for {
        n <- (0 until nbatches) .async
        k <- random.nextInts (nputs, nkeys) .latch.unit
      } {
        put (n + start, k, random.nextInt (1<<20))
      }

    def checkpoint () (implicit disks: Disks): Async [Unit] = {
      val save = attempted
      for {
        pos <- pagers.table.write (0, gen, save)
        _ <- records.checkpoint.record (gen, pos)
      } yield ()
    }

    def attach (implicit launch: Disks.Launch) {
      import launch.disks
      launch.checkpoint (checkpoint())
    }

    def check (found: Map [Int, Int]) {
      for (k <- accepted.keySet)
        assert (found contains k)
      for ((k, v) <- found)
        assert (attempted (k) == v || accepted (k) == v)
    }

    override def toString = s"Tracker(\n  $attempted,\n  $accepted)"
  }

  class LogReplayer {

    private var replayed = Map.empty [Int, Int] .withDefaultValue (-1)
    private var reread = Option.empty [Position]
    private var round = 0
    private var gen = 0

    def put (n: Int, g: Int, k: Int, v: Int) {
      assert (n >= round)
      round = n
      if (g > this.gen) {
        this.gen = g
        this.replayed = Map.empty
        this.reread = Option.empty
      }
      replayed += k -> v
    }

    def checkpoint (gen: Int, pos: Position) {
      if (gen > this.gen) {
        this.gen = gen
        this.replayed = Map.empty
        this.reread = Some (pos)
      } else if (gen == this.gen) {
        this.reread = Some (pos)
      }}

    def attach (implicit recovery: Disks.Recovery) {
      records.put.replay ((put _).tupled)
      records.checkpoint.replay ((checkpoint _).tupled)
    }

    def check (tracker: LogTracker) (implicit scheduler: StubScheduler, disks: Disks) {
      reread match {
        case Some (pos) =>
          val saved = pagers.table.read (pos) .pass
          tracker.check (saved ++ replayed)
        case None =>
          tracker.check (replayed)
      }}

    override def toString = s"Replayer(\n  $reread\n  $replayed)"
  }

  class StuffTracker (implicit random: Random) {

    private var written = Map.empty [Long, Position]
    private var _probed = false
    private var _compacted = false
    private var _maximum = 0L

    def probed = _probed
    def compacted = _compacted
    def maximum = _maximum

    def write() (implicit disks: Disks): Async [Unit] = {
      var seed = random.nextLong()
      while (written contains seed)
        seed = random.nextLong()
      for {
        pos <- pagers.stuff.write (0, seed, Stuff (seed))
      } yield {
        if (pos.offset > _maximum)
          _maximum = pos.offset
        written += seed -> pos
      }}

   def batch (nbatches: Int, nwrites: Int) (implicit
      scheduler: StubScheduler, disks: Disks): Async [Unit] =
    for {
      _ <- (0 until nbatches) .async
      _ <- (0 until nwrites) .latch.unit
    } {
      write()
    }

    def attach (implicit launch: Disks.Launch) {
      import launch.disks

      _probed = false
      _compacted = false

      pagers.stuff.handle (new PageHandler [Long] {

        def probe (obj: ObjectId, groups: Set [Long]): Async [Set [Long]] =
          supply {
            _probed = true
            val keep = groups filter (_ => random.nextInt (3) == 0)
            written = written filter (keep contains _)
            keep
          }

        def compact (obj: ObjectId, groups: Set [Long]): Async [Unit] =
          guard {
            _compacted = true
            for (seed <- groups.latch.unit)
              for (pos <- pagers.stuff.write (0, seed, Stuff (seed)))
                yield written += seed -> pos
          }})
    }

    def check () (implicit scheduler: StubScheduler, disks: Disks) {
      for ((seed, pos) <- written) {
        pagers.stuff.assertInLedger (pos, 0, seed)
        pagers.stuff.read (pos) .expect (Stuff (seed))
      }}}}
