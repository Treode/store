/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.disk

import scala.util.Random

import com.treode.async.Async
import com.treode.async.stubs.{AsyncChecks, StubScheduler}
import com.treode.async.stubs.implicits._
import org.scalatest.{Assertions, Informing, Suite}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.SpanSugar

import SpanSugar._

trait CrashChecks extends AsyncChecks with TimeLimitedTests {
  this: Suite with Informing =>

  override val timeLimit = 5 minutes

  class ForCrashesRunner (
      val messages: Seq [String],
      val setup: StubScheduler => Async [_],
      val asserts: Seq [Unit => Unit],
      val recover: StubScheduler => Any
  )

  class ForCrashesRecover (
      messages: Seq [String],
      setup: StubScheduler => Async [_]
  ) {

    private val asserts = Seq.newBuilder [Unit => Unit]

    def assert (cond: => Boolean, msg: String): ForCrashesRecover = {
      asserts += (_ => if (!cond) fail (msg))
      this
    }

    def recover (recover: StubScheduler => Any) =
      new ForCrashesRunner (messages, setup, asserts.result, recover)
  }

  class ForCrashesSetup {

    private val messages = Seq.newBuilder [String]

    def info (msg: String): ForCrashesSetup = {
      messages += msg
      this
    }

    def setup (setup: StubScheduler => Async [_]) =
      new ForCrashesRecover (messages.result, setup)
  }

  def crash: ForCrashesSetup =
    new ForCrashesSetup

  def setup (setup: StubScheduler => Async [_]) =
    new ForCrashesRecover (Seq.empty, setup)

  def forCrash (seed: Long, target: Int) (init: Random => ForCrashesRunner): Int = {

    val random = new Random (seed)
    val runner = init (random)

    try {
      val actual = {           // Setup running only the target number of tasks.
        val scheduler = StubScheduler.random (random)
        val cb = runner.setup (scheduler) .capture()
        val actual = scheduler.run (timers = !cb.wasInvoked, count = target)
        if (target == Int.MaxValue) {
          cb.assertPassed()
          runner.asserts foreach (_(()))
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

  def forAllCrashes (init: Random => ForCrashesRunner) {
    val start = System.currentTimeMillis

    for (_ <- 0 until nseeds  ) {

      val seed = Random.nextLong()
      val count = forCrash (seed , Int.MaxValue) (init)
      val random = new Random (seed)
      forCrash(seed, random.nextInt(count - 1) + 1) (init)

    }
    val end = System.currentTimeMillis
    val average = (end - start) / nseeds
    info (s"Crash and recovery average time ${average}ms")
  }}
