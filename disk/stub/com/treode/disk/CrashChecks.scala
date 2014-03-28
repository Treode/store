package com.treode.disk

import scala.util.Random

import com.treode.async.{Async, AsyncTestTools, StubScheduler}
import org.scalatest.{Informing, ParallelTestExecution, Suite}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.SpanSugar

import AsyncTestTools._
import SpanSugar._

trait CrashChecks extends ParallelTestExecution with TimeLimitedTests {
  this: Suite with Informing =>

  private val ncrashes = 100
  private val nseeds = 100

  val timeLimit = 5 minutes

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
        info (s"Crash and recovery failed; seed = ${seed}L, target = $target")
        t.printStackTrace()
        throw t
    }}

  def forAllCrashes (seed: Long) (init: Random => ForCrashesRunner): Int = {

    // Run the first time for as long as possible.
    val count = forCrash (seed, Int.MaxValue) (init)

    // Run the subsequent times for a portion of what was possible.
    if (count < ncrashes) {
      for (i <- 1 to count)
        forCrash (seed, i) (init)
      count
    } else {
      val random = new Random (seed)
      for (i <- 1 to ncrashes)
        forCrash (seed, random.nextInt (count - 1) + 1) (init)
      ncrashes
    }}

  def forAllCrashes (init: Random => ForCrashesRunner) {
    val start = System.currentTimeMillis
    var count = 0
    for (_ <- 0 until nseeds) {
      val seed = Random.nextLong()
      count += forAllCrashes (seed) (init)
    }
    val end = System.currentTimeMillis
    val average = (end - start) / count
    info (s"Crash and recovery average time ${average}ms")
  }}
