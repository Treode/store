package com.treode.disk

import com.treode.async.StubScheduler
import com.treode.async.io.StubFile
import com.treode.pickle.Picklers
import org.scalatest.FlatSpec

import DiskTestTools._

class RootSpec extends FlatSpec {

  implicit val config = DisksConfig (14, 1<<24, 1<<16)

  val geometry = DiskGeometry (10, 6, 1<<20)

  val root = new RootDescriptor (0x5FD8D9DF, Picklers.string)

  "The roots" should "work" in {
    implicit val scheduler = StubScheduler.random()
    val disk1 = new StubFile

    try {
    {
      implicit val recovery = Disks.recover()
      recovery.launch { implicit launcher =>
        root.checkpoint (_ ("one"))
        launcher.ready()
      }
      implicit val disks = recovery.attachAndLaunch (("a", disk1, geometry))
      disks.checkpoint()
      scheduler.runTasks()
      disks.assertLaunched (false)
    }

    {
      implicit val recovery = Disks.recover()
      var reloaded: String = null
      root.reload { s => implicit reloader =>
        reloaded = s
        reloader.ready()
      }
      implicit val disks = recovery.reattachAndPass (("a", disk1))
      expectResult ("one") (reloaded)
    }
    } catch {
      case e: Throwable =>
        //e.printStackTrace()
        throw e
    }
    }}
