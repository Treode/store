package com.treode.store.disk2

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

  "It" should "work" in {
    implicit val scheduler = StubScheduler.random()
    val disk1 = new StubFile (scheduler)
    val kit = new RichDisksKit (scheduler)
    kit.attachAndPass (("a", disk1, config))

    val seq = Seq (0, 1, 2)
    val pos = kit.writeAndPass (desc, 0, seq)
    expectResult (seq) (kit.readAndPass (desc, pos))
  }}
