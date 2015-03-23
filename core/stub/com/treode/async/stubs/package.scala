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

package com.treode.async

/** Almost everything of the async package uses a [[com.treode.async.Scheduler Scheduler]].
  * This provides a simple API that can be stubbed for testing.  The random
  * [[com.treode.async.stubs.StubScheduler$ StubScheduler]] can run tasks in the current
  * thread using a psuedo-random number generator to choose the next task.  Given the same seed it
  * will run a test repeatabily, and given different seeds it will simulate running simultaneous
  * tasks in a different order.
  *
  * The [[com.treode.async.stubs.AsyncCaptor AsyncCaptor]] and
  * [[com.treode.async.stubs.CallbackCaptor CallbackCaptor]] facilitate mocking and unit testing
  * asynchronous methods.
  *
  * Suppose we are testing the `CountDownLatch` example from [[com.treode.async.Fiber Fiber]].
  *
  * {{{
  * import com.treode.async.stubs.{AsyncChecks, StubScheduler}
  * import com.treode.async.stubs.implicits._
  * import com.treode.async.io.stubs.StubFile
  * import org.scalatest.FlatSpec
  *
  * class CountDownLatchSpec extends FlatSpec with AsyncChecks {
  *
  *   "It" should "work" in {
  *
  *     // The forAllRandoms method runs a test many times, each time with a
  *     // Random that has been seeded differently.  If the test throws an
  *     // error, forAllRandoms will inform you of which seed trigged the
  *     // problem.
  *     forAllRandoms { random =>
  *       implicit val scheduler = StubScheduler.random (random)
  *       implicit val latch = new CountDownLatch (2)
  *
  *       // Invoke await and give it a CallbackCaptor as its callback.
  *       val cb = latch.await().capture()
  *
  *       // The decrement method uses fiber.execute which schedules the task,
  *       // but the StubScheduler does not run it until its runTasks method is
  *       // called.
  *       latch.decrement()
  *       latch.decrement()
  *
  *       // The passed method on the CallbackCaptor runs all current and added
  *       // tasks; when none remain it checks that the callback was invoked
  *       // with Success.
  *       cb.passed
  *     } } }
  * }}}
  *
  * All classes in `stubs` packages are separated from the default Ivy package so that they do not
  * contaminate an assembly jar bound for production.  You want to setup the dependency in SBT
  * using:
  *
  * {{{
  * libraryDependencies += "com.treode" %% "store" % "compile;test->stub"
  * }}}
  */
package object stubs {}
