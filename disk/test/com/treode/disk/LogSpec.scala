package com.treode.disk

import scala.util.Random

import com.treode.async.{Async, AsyncConversions, AsyncTestTools, Callback, StubScheduler}
import com.treode.async.io.StubFile
import com.treode.pickle.{InvalidTagException, Picklers}
import com.treode.tags.{Intensive, Periodic}
import org.scalacheck.Gen
import org.scalatest.FlatSpec
import org.scalatest.prop.PropertyChecks

import Async.{async, latch}
import AsyncConversions._
import DiskTestTools._
import PropertyChecks._

class LogSpec extends FlatSpec with PropertyChecks {

  class DistinguishedException extends Exception

  implicit val config = TestDisksConfig()
  val geometry = TestDiskGeometry()
  val record = RecordDescriptor (0xBF, Picklers.string)
  val seeds = Gen.choose (0, Long.MaxValue)

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
      record.replay (replayed += _)
      recovery.reattachAndLaunch (("a", disk))
      assertResult (Seq.empty) (replayed.result)
    }}

  it should "replay one item" in {

    val disk = new StubFile () (null)

    {
      implicit val scheduler = StubScheduler.random()
      implicit val recovery = Disks.recover()
      implicit val disks = recovery.attachAndLaunch (("a", disk, geometry))
      record.record ("one") .pass
    }

    {
      implicit val scheduler = StubScheduler.random()
      implicit val recovery = Disks.recover()
      val replayed = Seq.newBuilder [String]
      record.replay (replayed += _)
      recovery.reattachAndLaunch (("a", disk))
      assertResult (Seq ("one")) (replayed.result)
    }}

  it should "report an unrecognized record" in {

    val disk = new StubFile () (null)

    {
      implicit val scheduler = StubScheduler.random()
      implicit val recovery = Disks.recover()
      implicit val disks = recovery.attachAndLaunch (("a", disk, geometry))
      record.record ("one") .pass
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
      record.record ("one") .pass
    }

    {
      implicit val scheduler = StubScheduler.random()
      implicit val recovery = Disks.recover()
      record.replay (_ => throw new DistinguishedException)
      recovery.reattachAndWait (("a", disk)) .fail [DistinguishedException]
    }}

  it should "run one checkpoint at a time" in {
    forAll (seeds) { seed =>

      implicit val random = new Random (seed)
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
    }}
}
