package com.treode.disk

import com.treode.async._
import com.treode.async.io.StubFile
import com.treode.pickle.Picklers
import org.scalatest.FlatSpec

class LogSpec extends FlatSpec {

  val config = DiskDriveConfig (6, 2, 1<<20)

  val root = new RootDescriptor (0xD6BA4C18, Picklers.string)

  val record = new RecordDescriptor (0x0E4F8ABF, Picklers.string)

  implicit class RichRecordDescriptor [R] (desc: RecordDescriptor [R]) {

    def recordAndPass (entry: R) (implicit scheduler: StubScheduler, disks: Disks) {
      val cb = new CallbackCaptor [Unit]
      desc.record (entry) (cb)
      scheduler.runTasks()
      cb.passed
    }}

  def replay (f: String => Any) (implicit disks: Disks): Unit =
    disks.open { implicit recovery =>
      record.replay (f)
    }

  "The logger" should "replay when reattaching disks" in {
    implicit val scheduler = StubScheduler.random()
    val disk1 = new StubFile

    {
      implicit val disks = new RichDisksKit
      replay (_ => fail ("Nothing to replay."))
      disks.attachAndPass (("a", disk1, config))
      record.recordAndPass ("one")
    }

    {
      implicit val disks = new RichDisksKit
      val replayed = Seq.newBuilder [String]
      replay (replayed += _)
      disks.reattachAndPass (("a", disk1))
      expectResult (Seq ("one")) (replayed.result)
    }}}
