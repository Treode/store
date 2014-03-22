package com.treode.disk

import scala.util.Random

import com.treode.async.{Async, AsyncConversions, AsyncTestTools, StubScheduler}
import com.treode.async.io.StubFile
import com.treode.pickle.Picklers
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.SpanSugar

import Async.async
import AsyncConversions._
import CrashChecks._
import DiskTestTools._
import DiskSystemSpec._
import SpanSugar._

class DiskSystemSpec extends FreeSpec with TimeLimitedTests {

  implicit val config = DisksConfig (0, 8, 1<<30, 1<<30, 1<<30, 1)
  val geometry = DiskGeometry (10, 6, 1<<20)

  val timeLimit = 5 minutes

  "The log" - {

    "without checkpoints, should replay items" taggedAs (Intensive, Periodic) in {
      forAllCrashes (0) { implicit random =>

        val nkeys = 100
        val nrounds = 100
        val nbatch = 10

        val disk = new StubFile () (null)
        val tracker = new Tracker

        setup { implicit scheduler =>
          disk.scheduler = scheduler

          implicit val recovery = Disks.recover()
          descriptor.replay (_ => fail ("Nothing to replay."))
          implicit val disks = recovery.attachAndLaunch (("a", disk, geometry))

          for (n <- (0 until nrounds) .async)
            random.nextInts (nbatch, nkeys) .latch.unit { k =>
              tracker.record (n, k, random.nextInt (1<<20))
            }}

        .recover { implicit scheduler =>
          disk.scheduler = scheduler

          implicit val recovery = Disks.recover()
          descriptor.replay ((tracker.replay _).tupled)
          implicit val disks = recovery.reattachAndLaunch (("a", disk))

          tracker.check()
        }}}}}

object DiskSystemSpec {

  val descriptor = {
    import Picklers._
    RecordDescriptor (0xBF, tuple (int, int, int))
  }

  class Tracker {

    private var attempted = Map.empty [Int, Int] .withDefaultValue (-1)
    private var accepted = Map.empty [Int, Int] .withDefaultValue (-1)
    private var replayed = Map.empty [Int, Int] .withDefaultValue (-1)
    private var round = 0

    def record (n: Int, k: Int, v: Int) (implicit disks: Disks): Async [Unit] = {
      attempted += k -> v
      for {
        _ <- descriptor.record (n, k, v)
      } yield {
        accepted += k -> v
      }}

    def replay (n: Int, k: Int, v: Int) {
      assert (n >= round)
      round = n
      replayed += k -> v
    }

    def check() {
      for ((k, v) <- replayed)
        assert (attempted (k) == v || accepted (k) == v)
    }}}
