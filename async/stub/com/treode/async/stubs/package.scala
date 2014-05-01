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
  *     // The forAllSeeds method runs a test many times, each time with a
  *     // Random that has been seeded differently.  If the test throws an
  *     // error, forAllSeeds will inform you of which seed trigged the
  *     // problem.
  *     forAllSeeds { random =>
  *       implicit val scheduler = StubScheduler.random (random)
  *       implicit val latch = new CountDownLatch (2)
  *
  *       // Invoke await and give it a CallbackCaptor as its callback.
  *       val cb = latch.await().capture()
  *
  *       // The decrement method uses fiber.execute which schedules the task,
  *       // but the StubScheduler does not run it until its runTasks method
  *       // is called.
  *       latch.decrement()
  *       latch.decrement()
  *
  *       // The passed method on the CallbackCaptor runs all current and
  *       // added tasks; when none remain it checks that the callback was
  *       // invoked with Success.
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
