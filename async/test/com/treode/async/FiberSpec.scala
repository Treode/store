package com.treode.async

import org.scalatest.FlatSpec

import Async.supply
import AsyncTestTools._
import Callback.{ignore => disreguard}

class FiberSpec extends FlatSpec {

  class DistinguishedException extends Exception

  val throwDistinguishedException =
    new Runnable {
      def run() = throw new DistinguishedException
    }

  "A Fiber" should "run one task" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    var a = false
    f.execute (a = true)
    assertResult (false) (a)
    s.runTasks()
    assertResult (true) (a)
  }

  it should "run two queued tasks" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    var a = false
    var b = false
    f.execute (a = true)
    f.execute (b = true)
    assertResult (false) (a)
    assertResult (false) (b)
    s.runTasks()
    assertResult (true) (a)
    assertResult (true) (b)
  }

  it should "run two tasks one after the other" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    var a = false
    var b = false
    f.execute (a = true)
    assertResult (false) (a)
    s.runTasks()
    assertResult (true) (a)
    f.execute (b = true)
    assertResult (false) (b)
    s.runTasks()
    assertResult (true) (b)
  }

  it should "handle the return keyword" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    var b = false
    def method(): Unit = f.execute { return; b = true }
    method()
    s.runTasks()
    assertResult (false) (b)
  }

  it should "report an exception thrown from a task and continue" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    var a = false
    f.execute (throwDistinguishedException)
    f.execute (a = true)
    assertResult (false) (a)
    intercept [DistinguishedException] (s.runTasks())
    s.runTasks()
    assertResult (true) (a)
  }

  "Fiber.async" should "not invoke the callback" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    var a = false
    val cb = f.async [Unit] (cb => a = true) .capture()
    assertResult (false) (a)
    cb.assertNotInvoked()
    s.runTasks()
    assertResult (true) (a)
    cb.assertNotInvoked()
  }

  it should "reject the return keyword" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    def method(): Async [Int] = f.async (_ => return null)
    method() .fail [ReturnNotAllowedFromAsync]
  }

  it should "report an exception through the callback" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    f.async [Unit] (cb => throw new DistinguishedException) .fail [DistinguishedException]
  }

  "Fiber.guard" should "report an exception through the callback" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    f.guard (throw new DistinguishedException) .fail [DistinguishedException]
  }

  it should "handle the return keyword" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    def method(): Async [Int] = f.guard (return supply (0))
    method() .expect (0)
  }

  "Fiber.supply" should "invoke the callback" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    var a = false
    f.supply (a = true) .pass
    assertResult (true) (a)
  }

  it should "reject the return keyword" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    def method(): Async [Int] = f.supply {return null}
    method() .fail [ReturnNotAllowedFromAsync]
  }

  it should "report an exception through the callback" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    f.supply (throw new DistinguishedException) .fail [DistinguishedException]
  }

  "Fiber.run" should "invoke the callback" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    var a = false
    val cb = CallbackCaptor [Unit]
    f.run [Unit] (cb) (supply (a = true))
    assertResult (false) (a)
    cb.assertNotInvoked()
    s.runTasks()
    assertResult (true) (a)
    cb.assertInvoked()
  }

  it should "reject the return keyword" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    val cb = CallbackCaptor [Unit]
    def method(): Unit = f.run [Unit] (cb) {return}
    method()
    cb.assertNotInvoked()
    s.runTasks()
    cb.failed [ReturnNotAllowedFromAsync]
  }

  it should "report an exception through the callback" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    val cb = CallbackCaptor [Unit]
    f.run [Unit] (cb) (throw new DistinguishedException)
    cb.assertNotInvoked()
    s.runTasks()
    cb.failed [DistinguishedException]
  }

  "Fiber.defer" should "not invoke the callback" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    var a = false
    val cb = CallbackCaptor [Unit]
    f.defer (cb) (a = true)
    assertResult (false) (a)
    cb.assertNotInvoked()
    s.runTasks()
    assertResult (true) (a)
    cb.assertNotInvoked()
  }

  it should "reject the return keyword" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    val cb = CallbackCaptor [Unit]
    def method(): Unit = f.defer (cb) {return}
    method()
    cb.assertNotInvoked()
    s.runTasks()
    cb.failed [ReturnNotAllowedFromAsync]
  }

  it should "report an exception through the callback" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    val cb = CallbackCaptor [Unit]
    f.defer (cb) (throw new DistinguishedException)
    cb.assertNotInvoked()
    s.runTasks()
    cb.failed [DistinguishedException]
  }

  "Fiber.invoke" should "invoke the callback" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    var a = false
    val cb = CallbackCaptor [Unit]
    f.invoke [Unit] (cb) (a = true)
    assertResult (false) (a)
    cb.assertNotInvoked()
    s.runTasks()
    assertResult (true) (a)
    cb.assertInvoked()
  }

  it should "reject the return keyword" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    val cb = CallbackCaptor [Unit]
    def method(): Unit = f.invoke [Unit] (cb) {return}
    method()
    cb.assertNotInvoked()
    s.runTasks()
    cb.failed [ReturnNotAllowedFromAsync]
  }

  it should "report an exception through the callback" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber (s)
    val cb = CallbackCaptor [Unit]
    f.defer (cb) (throw new DistinguishedException)
    cb.assertNotInvoked()
    s.runTasks()
    cb.failed [DistinguishedException]
  }}
