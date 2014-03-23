package com.treode.disk

import scala.util.Random

import com.treode.async.{Async, AsyncTestTools, StubScheduler}
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks

import AsyncTestTools._

trait CrashChecks extends PropertyChecks {

  private val seeds = Gen.choose (0L, Long.MaxValue)

  class ForCrashesRunner (
      val setup: StubScheduler => Async [_],
      val recover: StubScheduler => Any
  )

  class ForCrashesSetup (setup: StubScheduler => Async [_]) {
    def recover (recover: StubScheduler => Any) =
      new ForCrashesRunner (setup, recover)
  }

  def setup (setup: StubScheduler => Async [_]) (implicit random: Random) =
    new ForCrashesSetup (setup)

  def forCrash (seed: Long, target: Int) (init: Random => ForCrashesRunner): Int = {

    val random = new Random (seed)
    val runner = init (random)

    val actual = {           // Setup running only the target number of tasks.
      val scheduler = StubScheduler.random (random)
      val cb = runner.setup (scheduler) .capture()
      scheduler.runTasks (count = target)
    }

    {                        // Then check the recovery.
      val scheduler = StubScheduler.random (random)
      runner.recover (scheduler)
    }

    actual
  }

  def forAllCrashes (seed: Long) (init: Random => ForCrashesRunner) {

    // Run the first time for as long as possible.
    val count = forCrash (seed, Int.MaxValue) (init)

    // Run the subsequent times for a portion of what was possible.
    for (i <- 1 to count)
      forCrash (seed, i) (init)
  }

  def forAllCrashes (init: Random => ForCrashesRunner) {
    forAll (seeds -> "seed") { seed: Long =>
      println (f"Checking crashes with seed 0x$seed%016X")
      forAllCrashes (seed) (init)
    }}}

object CrashChecks extends CrashChecks
