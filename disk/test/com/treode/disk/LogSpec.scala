package com.treode.disk

import com.treode.async.{Async, AsyncTestTools, StubScheduler}
import com.treode.async.io.StubFile
import com.treode.pickle.{InvalidTagException, Picklers}
import org.scalatest.FlatSpec

import Async.async
import DiskTestTools._

class LogSpec extends FlatSpec {

  class DistinguishedException extends Exception

  implicit val config = DisksConfig (0, 8, 1<<30, 1<<30, 1<<30, 1)
  val geometry = DiskGeometry (10, 6, 1<<20)
  val record = RecordDescriptor (0xBF, Picklers.string)

  "The logger" should "replay zero items" in {

    val disk = new StubFile () (null)

    {
      implicit val scheduler = StubScheduler.random()
      disk.scheduler = scheduler
      implicit val recovery = Disks.recover()
      implicit val disks = recovery.attachAndLaunch (("a", disk, geometry))
    }

    {
      implicit val scheduler = StubScheduler.random()
      disk.scheduler = scheduler
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
      disk.scheduler = scheduler
      implicit val recovery = Disks.recover()
      implicit val disks = recovery.attachAndLaunch (("a", disk, geometry))
      record.record ("one") .pass
    }

    {
      implicit val scheduler = StubScheduler.random()
      disk.scheduler = scheduler
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
      disk.scheduler = scheduler
      implicit val recovery = Disks.recover()
      implicit val disks = recovery.attachAndLaunch (("a", disk, geometry))
      record.record ("one") .pass
    }

    {
      implicit val scheduler = StubScheduler.random()
      disk.scheduler = scheduler
      implicit val recovery = Disks.recover()
      recovery.reattachAndCapture (("a", disk)) .fail [InvalidTagException]
    }}

  it should "report an error from a replay function" in {

    val disk = new StubFile () (null)

    {
      implicit val scheduler = StubScheduler.random()
      disk.scheduler = scheduler
      implicit val recovery = Disks.recover()
      implicit val disks = recovery.attachAndLaunch (("a", disk, geometry))
      record.record ("one") .pass
    }

    {
      implicit val scheduler = StubScheduler.random()
      disk.scheduler = scheduler
      implicit val recovery = Disks.recover()
      record.replay (_ => throw new DistinguishedException)
      recovery.reattachAndCapture (("a", disk)) .fail [DistinguishedException]
    }}
}
