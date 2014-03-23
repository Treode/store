package com.treode.disk

import scala.util.Random

import com.treode.async.{Async, AsyncTestTools, StubScheduler}
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks

import AsyncTestTools._

trait CrashChecks extends PropertyChecks {

  private val seeds = Gen.choose (0L, Long.MaxValue)

  private val limit = 100

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

  def forCrash (random: Random, target: Int) (init: Random => ForCrashesRunner): Int = {

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

    val random = new Random (seed)

    // Run the first time for as long as possible.
    val count = forCrash (random, Int.MaxValue) (init)

    // Run the subsequent times for a portion of what was possible.
    if (count < limit)
      for (i <- 1 to count)
        forCrash (random, i) (init)
    else
      for (i <- 1 to limit)
        forCrash (random, random.nextInt (count - 1) + 1) (init)
  }

  def forAllCrashes (init: Random => ForCrashesRunner) {
    forAll (seeds -> "seed") { seed: Long =>
      println (f"Checking crashes with seed 0x$seed%016X")
      forAllCrashes (seed) (init)
    }}}

object CrashChecks extends CrashChecks
