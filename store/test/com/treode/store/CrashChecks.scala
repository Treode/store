package com.treode.store

import scala.util.Random

import com.treode.async.{Async, AsyncTestTools, StubScheduler}
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks

import AsyncTestTools._

trait CrashChecks extends PropertyChecks {

  private val seeds = Gen.choose (0L, Long.MaxValue)

  class ForAllCrashesRunner (
      val setup: StubScheduler => Async [_],
      val recover: StubScheduler => Any
  )

  class ForAllCrashesSetup (setup: StubScheduler => Async [_]) {
    def recover (recover: StubScheduler => Any) =
      new ForAllCrashesRunner (setup, recover)
  }

  def setup (setup: StubScheduler => Async [_]) (implicit random: Random) =
    new ForAllCrashesSetup (setup)

  def forAllCrashes (seed: Long) (init: Random => ForAllCrashesRunner) {

    val random = new Random (seed)
    val runner = init (random)

    val count = { // Run the first time for as long as possible.
      val scheduler = StubScheduler.random (random)
      val cb = runner.setup (scheduler) .capture()
      val count = scheduler.runTasks()
      cb.passed
      count
    }

    { // And check that it can recover.
      val scheduler = StubScheduler.random (random)
      runner.recover (scheduler)
    }

    for (i <- 1 to count) { // For each portion of what was possible.

      val random = new Random (seed)
      val runner = init (random)

      { // Run the second time only part of what was possible.
        val scheduler = StubScheduler.random (random)
        val cb = runner.setup (scheduler) .capture()
        scheduler.runTasks (count = i)
      }

      { // And check that it can recover.
        val scheduler = StubScheduler.random (random)
        runner.recover (scheduler)
      }}}

  def forAllCrashes (init: Random => ForAllCrashesRunner) {
    forAll (seeds -> "seed") { seed: Long =>
      println (f"Checking crashes with seed 0x$seed%016X")
      forAllCrashes (seed) (init)
    }}}
