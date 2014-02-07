package com.treode.disk

import com.treode.async.{Callback, StubScheduler}
import com.treode.async.io.StubFile
import com.treode.pickle.Picklers
import org.scalatest.FlatSpec

import DiskTestTools._

class PageSpec extends FlatSpec {

  val config = DiskDriveConfig (10, 6, 1<<20)

  val desc = {
    import Picklers._
    new PageDescriptor (0xF2D7C1E9, int, seq (int))
  }

  "The pager" should "fetch after write and restart" in {
    implicit val scheduler = StubScheduler.random()
    val disk1 = new StubFile
    val seq = Seq (0, 1, 2)
    var pos = Position (0, 0, 0)

    {
      implicit val recovery = Disks.recover()
      implicit val disks = recovery.attachAndLaunch (("a", disk1, config))
      pos = disks.writeAndPass (desc, 0, seq)
      expectResult (seq) (disks.readAndPass (desc, pos))
    }

    {
      implicit val recovery = Disks.recover()
      implicit val disks = recovery.reattachAndPass (("a", disk1))
      expectResult (seq) (disks.readAndPass (desc, pos))
    }}}
