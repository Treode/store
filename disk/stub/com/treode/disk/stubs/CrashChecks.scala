package com.treode.disk.stubs

import scala.util.Random

import com.treode.async.Async
import com.treode.async.stubs.{AsyncChecks, StubScheduler}
import com.treode.async.stubs.implicits._
import org.scalatest.{Assertions, Informing, Suite}

trait CrashChecks extends AsyncChecks {
  this: Suite with Informing =>

  private val ncrashes =
    intensity match {
      case "development" => 1
      case _ => 10
    }

  class ForCrashesRunner (
      val messages: Seq [String],
      val setup: StubScheduler => Async [_],
      val asserts: Seq [Unit => Unit],
      val recover: StubScheduler => Any
  )

  class ForCrashesSetup (
      messages: Seq [String],
      setup: StubScheduler => Async [_]
  ) {

    private val asserts = Seq.newBuilder [Unit => Unit]

    def assert (cond: => Boolean, msg: String): ForCrashesSetup = {
      asserts += (_ => Assertions.assert (cond, msg))
      this
    }

    def recover (recover: StubScheduler => Any) =
      new ForCrashesRunner (messages, setup, asserts.result, recover)
  }

  class ForCrashesInfo {

    private val messages = Seq.newBuilder [String]

    def info (msg: String): ForCrashesInfo = {
      messages += msg
      this
    }

    def setup (setup: StubScheduler => Async [_]) (implicit random: Random) =
      new ForCrashesSetup (messages.result, setup)
  }

  def crash: ForCrashesInfo =
    new ForCrashesInfo

  def setup (setup: StubScheduler => Async [_]) (implicit random: Random) =
    new ForCrashesSetup (Seq.empty, setup)

  def forCrash (seed: Long, target: Int) (init: Random => ForCrashesRunner): Int = {

    val random = new Random (seed)
    val runner = init (random)

    try {
      val actual = {           // Setup running only the target number of tasks.
        val scheduler = StubScheduler.random (random)
        val cb = runner.setup (scheduler) .capture()
        val actual = scheduler.run (count = target)
        if (target == Int.MaxValue) {
          cb.passed
          runner.asserts foreach (_())
        }
        actual
      }

      {                        // Then check the recovery.
        val scheduler = StubScheduler.random (random)
        runner.recover (scheduler)
      }

      actual
    } catch {
      case t: Throwable =>
        info (s"forCrash (seed = ${seed}L, target = $target)")
        runner.messages foreach (info (_))
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
    for (_ <- 0 until nseeds / ncrashes) {
      val seed = Random.nextLong()
      count += forAllCrashes (seed) (init)
    }
    val end = System.currentTimeMillis
    val average = (end - start) / count
    info (s"Crash and recovery average time ${average}ms")
  }}
