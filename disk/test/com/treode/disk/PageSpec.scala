package com.treode.disk

import com.treode.async.{Callback, StubScheduler}
import com.treode.async.io.StubFile
import com.treode.pickle.Picklers
import org.scalatest.FlatSpec

import DiskTestTools._

class PageSpec extends FlatSpec {

  implicit val config = DisksConfig (14, 1<<24, 1<<16, 10, 1)

  val geometry = DiskGeometry (10, 6, 1<<20)

  val desc = {
    import Picklers._
    PageDescriptor (0xF2D7C1E9, int, seq (int))
  }

  "The pager" should "fetch after write and restart" in {
    implicit val scheduler = StubScheduler.random()
    val disk1 = new StubFile
    val seq = Seq (0, 1, 2)
    var pos = Position (0, 0, 0)

    {
      implicit val recovery = Disks.recover()
      implicit val disks = recovery.attachAndLaunch (("a", disk1, geometry))
      pos = disks.write (desc, 0, seq) .pass
      expectResult (seq) (disks.read (desc, pos) .pass)
    }

    {
      implicit val recovery = Disks.recover()
      implicit val disks = recovery.reattachAndLaunch (("a", disk1))
      expectResult (seq) (disks.read (desc, pos). pass)
    }}}
