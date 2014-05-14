package com.treode.async.stubs

import java.util.concurrent.Executors
import scala.util.Random

import com.treode.async.Scheduler
import org.scalatest.{Informing, Suite}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.SpanSugar

import SpanSugar._

trait AsyncChecks extends TimeLimitedTests {
  this: Suite with Informing =>

  val timeLimit = 5 minutes

  /** The value of `TEST_INTENSITY` from the environment, or `standard` if the environment has
    * no setting for that.
    */
  val intensity: String = {
    val env = System.getenv ("TEST_INTENSITY")
    if (env == null) "standard" else env
  }

  /** The number of seeds tried in `forAllSeeds`.  1 when when `intensity` is `development` and
    * 100 otherwise.
    */
  val nseeds =
    intensity match {
      case "development" => 1
      case _ => 100
    }

  /** Run the test with a PRNG seeded by `seed`. */
  def forSeed (seed: Long) (test: Random => Any) {
    try {
      val random = new Random (seed)
      test (random)
    } catch {
      case t: Throwable =>
        info (s"Test failed; seed = ${seed}L")
        t.printStackTrace()
        throw t
    }}

  /** Run the test many times, each time with a PRNG seeded differently.  When developing, set
    * the environment variable `TEST_INTENSITY` to `development` to run the test only once.  Let
    * your continuous build spend the time running it over and over.
    */
  def forAllSeeds (test: Random => Any) {
    for (_ <- 0 until nseeds)
      forSeed (Random.nextLong) (test)
  }

  /** Run the test with a multithreaded scheduler, and then shutdown the underlying executor after
    * the test completes.
    */
  def multithreaded (test: StubScheduler => Any) {
    val processors = Runtime.getRuntime.availableProcessors
    val threads = if (processors < 8) 4 else 8
    val executor = Executors.newScheduledThreadPool (threads)
    try {
      test (StubScheduler.multithreaded (executor))
    } finally {
      executor.shutdown()
    }}}
