package com.treode.disk

import com.treode.async.StubScheduler
import com.treode.async.io.StubFile
import com.treode.pickle.Picklers
import org.scalatest.FlatSpec

class RootSpec extends FlatSpec {

  val config = DiskDriveConfig (6, 2, 1<<20)

  val desc = new RootDescriptor (0x5FD8D9DF, Picklers.string)

  "The roots" should "work" in {
    implicit val scheduler = StubScheduler.random()
    val disk1 = new StubFile

    {
      implicit val disks = new RichDisksKit
      desc.checkpoint (_ ("one"))
      desc.recover (_ => fail ("Nothing to recover."))
      disks.attachAndPass (("a", disk1, config))
      disks.checkpointAndPass()
    }

    {
      implicit val disks = new RichDisksKit
      var recovered: String = null
      desc.recover (recovered = _)
      disks.reattachAndPass (("a", disk1))
      expectResult ("one") (recovered)
    }}}
