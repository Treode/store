package com.treode.disk

import scala.util.Random

import com.treode.async.{Async, AsyncConversions, Callback, StubScheduler}
import com.treode.async.io.StubFile
import org.scalacheck.Gen
import org.scalatest.FlatSpec
import org.scalatest.prop.PropertyChecks

import Async.async
import AsyncConversions._
import DiskTestTools._
import PropertyChecks._

class CheckpointerSpec extends FlatSpec {

  implicit val config = DisksConfig (0, 8, 1<<30, 1<<30, 1<<30, 1)
  val geometry = DiskGeometry (10, 6, 1<<20)
  val seeds = Gen.choose (0, Int.MaxValue)

  "The Checkpointer" should "run one checkpoint at a time" in {
    forAll (seeds) { seed =>

      implicit val random = new Random (seed)
      implicit val scheduler = StubScheduler.random (random)
      val disk = new StubFile
      val recovery = Disks.recover()
      val launch = recovery.attachAndCapture (("a", disk, geometry)) .pass

      var checkpointed = false
      var cb: Callback [Unit] = null
      launch.checkpoint (async [Unit] { _cb =>
        assert (cb == null, "Expected no callback.")
        checkpointed = true
        cb = _cb.leave (cb = null)
      })
      launch.launch()

      import launch.kit.checkpointer.checkpoint
      checkpoint()
      checkpoint()
      checkpoint()
      scheduler.runTasks()
      assert (checkpointed, "Expected a checkpoint")
    }}
}
