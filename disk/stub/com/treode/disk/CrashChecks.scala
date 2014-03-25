package com.treode.disk

import scala.util.Random

import com.treode.async.{Async, AsyncTestTools, StubScheduler}
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks

import AsyncTestTools._

trait CrashChecks extends PropertyChecks {

  private val limit = 100

  val seeds = Gen.choose (0L, Long.MaxValue)

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
    try {

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
    } catch {
      case t: Throwable =>
        println (s"Crash and recovery failed; seed=$seed, target=$target")
        t.printStackTrace()
        throw t
    }}

  def forAllCrashes (seed: Long, quick: Boolean) (init: Random => ForCrashesRunner) {

    val start = System.currentTimeMillis

    // Run the first time for as long as possible.
    val count = forCrash (seed, Int.MaxValue) (init)

    // Run the subsequent times for a portion of what was possible.
    if (count < limit) {
      for (i <- 1 to count)
        forCrash (seed, i) (init)
    } else {
      val random = new Random (seed)
      for (i <- 1 to limit)
        forCrash (seed, random.nextInt (count - 1) + 1) (init)
    }

    val end = System.currentTimeMillis
    val average = (end - start) / (if (count < limit) count else limit)
    if (!quick)
      println (s"Crash and recovery: seed=$seed, time=${average}ms")
  }

  def forAllCrashes (init: Random => ForCrashesRunner) {
    forAll (seeds -> "seed") { seed: Long =>
      forAllCrashes (seed, false) (init)
    }}

  def forAllQuickCrashes (init: Random => ForCrashesRunner) {
    forAll (seeds -> "seed") { seed: Long =>
      forAllCrashes (seed, true) (init)
    }}}

object CrashChecks extends CrashChecks
