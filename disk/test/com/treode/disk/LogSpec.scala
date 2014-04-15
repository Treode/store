package com.treode.disk

import scala.util.Random

import com.treode.async._
import com.treode.async.io.StubFile
import com.treode.pickle.{InvalidTagException, Picklers}
import com.treode.tags.Periodic
import org.scalatest.FlatSpec

import Async.{async, latch}
import AsyncImplicits._
import DiskTestTools._

class LogSpec extends FlatSpec with CrashChecks {

  class DistinguishedException extends Exception

  implicit val config = TestDisksConfig()
  val geometry = TestDiskGeometry()

  object records {
    val str = RecordDescriptor (0xBF, Picklers.string)
    val stuff = RecordDescriptor (0x2B, Stuff.pickler)
  }

  "The logger" should "replay zero items" in {

    val disk = new StubFile () (null)

    {
      implicit val scheduler = StubScheduler.random()
      implicit val recovery = Disks.recover()
      implicit val disks = recovery.attachAndLaunch (("a", disk, geometry))
    }

    {
      implicit val scheduler = StubScheduler.random()
      implicit val recovery = Disks.recover()
      val replayed = Seq.newBuilder [String]
      records.str.replay (replayed += _)
      recovery.reattachAndLaunch (("a", disk))
      assertResult (Seq.empty) (replayed.result)
    }}

  it should "replay one item" in {

    val disk = new StubFile () (null)

    {
      implicit val scheduler = StubScheduler.random()
      implicit val recovery = Disks.recover()
      implicit val disks = recovery.attachAndLaunch (("a", disk, geometry))
      records.str.record ("one") .pass
    }

    {
      implicit val scheduler = StubScheduler.random()
      implicit val recovery = Disks.recover()
      val replayed = Seq.newBuilder [String]
      records.str.replay (replayed += _)
      recovery.reattachAndLaunch (("a", disk))
      assertResult (Seq ("one")) (replayed.result)
    }}

  it should "replay twice" in {

    val disk = new StubFile () (null)

    {
      implicit val scheduler = StubScheduler.random()
      implicit val recovery = Disks.recover()
      implicit val disks = recovery.attachAndLaunch (("a", disk, geometry))
      records.str.record ("one") .pass
    }

    {
      implicit val scheduler = StubScheduler.random()
      implicit val recovery = Disks.recover()
      val replayed = Seq.newBuilder [String]
      records.str.replay (replayed += _)
      implicit val disks = recovery.reattachAndLaunch (("a", disk))
      assertResult (Seq ("one")) (replayed.result)
      records.str.record ("two") .pass
    }

    {
      implicit val scheduler = StubScheduler.random()
      implicit val recovery = Disks.recover()
      val replayed = Seq.newBuilder [String]
      records.str.replay (replayed += _)
      recovery.reattachAndLaunch (("a", disk))
      assertResult (Seq ("one", "two")) (replayed.result)
    }}

  it should "report an unrecognized record" in {

    val disk = new StubFile () (null)

    {
      implicit val scheduler = StubScheduler.random()
      implicit val recovery = Disks.recover()
      implicit val disks = recovery.attachAndLaunch (("a", disk, geometry))
      records.str.record ("one") .pass
    }

    {
      implicit val scheduler = StubScheduler.random()
      implicit val recovery = Disks.recover()
      recovery.reattachAndWait (("a", disk)) .fail [InvalidTagException]
    }}

  it should "report an error from a replay function" in {

    val disk = new StubFile () (null)

    {
      implicit val scheduler = StubScheduler.random()
      implicit val recovery = Disks.recover()
      implicit val disks = recovery.attachAndLaunch (("a", disk, geometry))
      records.str.record ("one") .pass
    }

    {
      implicit val scheduler = StubScheduler.random()
      implicit val recovery = Disks.recover()
      records.str.replay (_ => throw new DistinguishedException)
      recovery.reattachAndWait (("a", disk)) .fail [DistinguishedException]
    }}

  it should "reject an oversized record" in {

    val disk = new StubFile () (null)

    {
      implicit val scheduler = StubScheduler.random()
      implicit val recovery = Disks.recover()
      implicit val disks = recovery.attachAndLaunch (("a", disk, geometry))
      records.stuff.record (Stuff (0, 1000)) .fail [OversizedRecordException]
    }}


  it should "run one checkpoint at a time" taggedAs (Periodic) in {
    forAllSeeds { implicit random =>

      implicit val scheduler = StubScheduler.random (random)
      val disk = new StubFile
      val recovery = Disks.recover()
      val launch = recovery.attachAndWait (("a", disk, geometry)) .pass
      import launch.disks

      var checkpointed = false
      var checkpointing = false
      launch.checkpoint (async [Unit] { cb =>
        assert (!checkpointing, "Expected one checkpoint at a time.")
        scheduler.execute {
          checkpointing = false
          cb.pass()
        }
        checkpointed = true
        checkpointing = true
      })
      launch.launch()
      scheduler.runTasks()

      latch (
          disks.checkpoint(),
          disks.checkpoint(),
          disks.checkpoint()) .pass
      assert (checkpointed, "Expected a checkpoint")
    }}}
