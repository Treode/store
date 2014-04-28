package com.treode.async.stubs

import scala.util.Random

import org.scalatest.{Informing, ParallelTestExecution, Suite}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.SpanSugar

import AsyncTestTools._
import SpanSugar._

trait AsyncChecks extends ParallelTestExecution with TimeLimitedTests {
  this: Suite with Informing =>

  val timeLimit = 5 minutes

  val intensity: String = {
    val env = System.getenv ("TEST_INTENSITY")
    if (env == null) "standard" else env
  }

  val nseeds =
    intensity match {
      case "development" => 1
      case _ => 100
    }

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

  def forAllSeeds (test: Random => Any) {
    for (_ <- 0 until nseeds)
      forSeed (Random.nextLong) (test)
  }}
