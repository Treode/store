package com.treode.disk

import com.treode.async.{Async, AsyncTestTools, StubScheduler}
import com.treode.async.io.StubFile
import com.treode.pickle.Picklers
import org.scalatest.FlatSpec

import Async.async
import DiskTestTools._

class LogSpec extends FlatSpec {

  implicit val config = DisksConfig (14, 1<<24, 1<<16, 10, 1)

  val geometry = DiskGeometry (10, 6, 1<<20)
  val record = RecordDescriptor (0xBF, Picklers.string)

  "The logger" should "replay when reattaching disks" in {
    implicit val scheduler = StubScheduler.random()
    val disk1 = new StubFile

    {
      implicit val recovery = Disks.recover()
      record.replay (_ => fail ("Nothing to replay."))
      implicit val disks = recovery.attachAndLaunch (("a", disk1, geometry))
      record.record ("one") .pass
    }

    {
      implicit val recovery = Disks.recover()
      val replayed = Seq.newBuilder [String]
      record.replay (replayed += _)
      implicit val disks = recovery.reattachAndLaunch (("a", disk1))
      assertResult (Seq ("one")) (replayed.result)
    }}}
