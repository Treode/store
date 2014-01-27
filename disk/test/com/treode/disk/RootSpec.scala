package com.treode.disk

import com.treode.async.StubScheduler
import com.treode.async.io.StubFile
import com.treode.pickle.Picklers
import org.scalatest.FlatSpec

class RootSpec extends FlatSpec {

  val config = DiskDriveConfig (6, 2, 1<<20)

  val root = new RootDescriptor (0x5FD8D9DF, Picklers.string)

  def reload (f: String => Any) (implicit disks: Disks): Unit =
    disks.open { implicit recovery =>
      root.reload { (s, cb) =>
        f (s)
        cb()
      }}

  "The roots" should "work" in {
    implicit val scheduler = StubScheduler.random()
    val disk1 = new StubFile

    {
      implicit val disks = new RichDisksKit
      root.checkpoint (_ ("one"))
      reload (_ => fail ("Nothing to reload."))
      disks.attachAndPass (("a", disk1, config))
      disks.checkpointAndPass()
    }

    {
      implicit val disks = new RichDisksKit
      var reloaded: String = null
      reload (reloaded = _)
      disks.reattachAndPass (("a", disk1))
      expectResult ("one") (reloaded)
    }}}
