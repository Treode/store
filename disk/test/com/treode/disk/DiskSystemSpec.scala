package com.treode.disk

import scala.util.Random

import com.treode.async.{Async, AsyncConversions, AsyncTestTools, StubScheduler}
import com.treode.async.io.StubFile
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.{FreeSpec, ParallelTestExecution}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.SpanSugar

import Async.supply
import AsyncConversions._
import CrashChecks._
import DiskTestTools._
import DiskSystemSpec._
import SpanSugar._

class DiskSystemSpec extends FreeSpec with ParallelTestExecution with TimeLimitedTests {

  val timeLimit = 5 minutes

  "The logger should replay items" - {

    "without checkpoints" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DisksConfig (0, 8, 1<<30, 1<<30, 1<<30, 1)
        val geometry = DiskGeometry (10, 6, 1<<20)
        val disk = new StubFile () (null)
        val tracker = new Tracker

        setup { implicit scheduler =>
          disk.scheduler = scheduler

          implicit val recovery = Disks.recover()
          val launch = recovery.attachAndCapture (("a", disk, geometry)) .pass
          import launch.disks
          launch.checkpoint (fail ("Expected no checkpoints"))
          launch.launch()
          tracker.batch (100, 40, 10)
        }

        .recover { implicit scheduler =>
          disk.scheduler = scheduler

          implicit val recovery = Disks.recover()
          val replayer = new Replayer
          replayer.attach (recovery)
          implicit val disks = recovery.reattachAndLaunch (("a", disk))
          replayer.check (tracker)
        }}}

    "with checkpoints" taggedAs (Intensive, Periodic) in {
      forAllCrashes { implicit random =>

        implicit val config = DisksConfig (0, 8, 1<<30, 17, 1<<30, 1)
        val geometry = DiskGeometry (10, 6, 1<<20)
        val disk = new StubFile () (null)
        val tracker = new Tracker

        setup { implicit scheduler =>
          disk.scheduler = scheduler

          implicit val recovery = Disks.recover()
          implicit val launch = recovery.attachAndCapture (("a", disk, geometry)) .pass
          import launch.disks
          var checkpoint = false
          tracker.attach (launch)
          launch.checkpoint (supply (checkpoint = true))
          launch.launch()
          for {
            _ <- tracker.batch (100, 40, 10)
          } yield {
            assert (checkpoint, "Expected a checkpoint")
          }}

        .recover { implicit scheduler =>
          disk.scheduler = scheduler

          implicit val recovery = Disks.recover()
          val replayer = new Replayer
          replayer.attach (recovery)
          implicit val disks = recovery.reattachAndLaunch (("a", disk))
          replayer.check (tracker)
        }}}}}

object DiskSystemSpec {

  object records {
    import DiskPicklers._

    val put =
      RecordDescriptor (0xBF, tuple (uint, uint, uint, uint))

    val checkpoint =
      RecordDescriptor (0x9B, tuple (uint, pos))
  }

  val pager = {
    import DiskPicklers._
    PageDescriptor (0x79, uint, map (uint, uint))
  }

  class Tracker {

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

    def batch (nkeys: Int, nrounds: Int, nbatch: Int) (
        implicit random: Random, scheduler: StubScheduler, disks: Disks): Async [Unit] =
      for {
        n <- (0 until nrounds) .async
        k <- random.nextInts (nbatch, nkeys) .latch.unit
      } {
        put (n, k, random.nextInt (1<<20))
      }

    def checkpoint () (implicit disks: Disks): Async [Unit] = {
      val save = attempted
      for {
        pos <- pager.write (0, gen, save)
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

  class Replayer {

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

    def attach (implicit disks: Disks.Recovery) {
      records.put.replay ((put _).tupled)
      records.checkpoint.replay ((checkpoint _).tupled)
    }

    def check (tracker: Tracker) (implicit scheduler: StubScheduler, disks: Disks) {
      reread match {
        case Some (pos) =>
          val saved = pager.read (pos) .pass
          tracker.check (saved ++ replayed)
        case None =>
          tracker.check (replayed)
      }}

    override def toString = s"Replayer(\n  $reread\n  $replayed)"
  }}
