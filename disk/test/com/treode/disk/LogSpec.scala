package com.treode.disk

import com.treode.async._
import com.treode.async.io.StubFile
import com.treode.pickle.Picklers
import org.scalatest.FlatSpec

import DiskTestTools._

class LogSpec extends FlatSpec {

  implicit val config = DisksConfig (14, 1<<24, 1<<16, 10, 1)

  val geometry = DiskGeometry (10, 6, 1<<20)
  val root = RootDescriptor (0xD6BA4C18, Picklers.string)
  val record = RecordDescriptor (0x0E4F8ABF, Picklers.string)

  implicit class RichRecordDescriptor [R] (desc: RecordDescriptor [R]) {

    def recordAndPass (entry: R) (implicit scheduler: StubScheduler, disks: Disks): Unit =
      CallbackCaptor.pass [Unit] (desc.record (entry) _)
  }

  "The logger" should "replay when reattaching disks" in {
    implicit val scheduler = StubScheduler.random()
    val disk1 = new StubFile

    {
      implicit val recovery = Disks.recover()
      record.replay (_ => fail ("Nothing to replay."))
      implicit val disks = recovery.attachAndLaunch (("a", disk1, geometry))
      record.recordAndPass ("one")
    }

    {
      implicit val recovery = Disks.recover()
      val replayed = Seq.newBuilder [String]
      record.replay (replayed += _)
      implicit val disks = recovery.reattachAndPass (("a", disk1))
      expectResult (Seq ("one")) (replayed.result)
    }}}
