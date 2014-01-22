package com.treode.disk

import com.treode.async.{Callback, StubScheduler}
import com.treode.async.io.StubFile
import com.treode.pickle.Picklers
import org.scalatest.FlatSpec

class PageSpec extends FlatSpec {

  val config = DiskDriveConfig (16, 12, 1L<<20)

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
      val kit = new RichDisksKit
      kit.attachAndPass (("a", disk1, config))
      pos = kit.writeAndPass (desc, 0, seq)
      expectResult (seq) (kit.readAndPass (desc, pos))
    }

    {
      val kit = new RichDisksKit
      kit.reattachAndPass (("a", disk1))
      expectResult (seq) (kit.readAndPass (desc, pos))
    }}}
