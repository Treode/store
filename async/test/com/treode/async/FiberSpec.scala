package com.treode.async

import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import org.scalatest.FlatSpec

import Async.supply
import Callback.{ignore => disreguard}

class FiberSpec extends FlatSpec {

  class DistinguishedException extends Exception

  val throwDistinguishedException =
    new Runnable {
      def run() = throw new DistinguishedException
    }

  "A Fiber" should "run one task" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    var a = false
    f.execute (a = true)
    assertResult (false) (a)
    s.run()
    assertResult (true) (a)
  }

  it should "run two queued tasks" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    var a = false
    var b = false
    f.execute (a = true)
    f.execute (b = true)
    assertResult (false) (a)
    assertResult (false) (b)
    s.run()
    assertResult (true) (a)
    assertResult (true) (b)
  }

  it should "run two tasks one after the other" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    var a = false
    var b = false
    f.execute (a = true)
    assertResult (false) (a)
    s.run()
    assertResult (true) (a)
    f.execute (b = true)
    assertResult (false) (b)
    s.run()
    assertResult (true) (b)
  }

  it should "handle the return keyword" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    var b = false
    def method(): Unit = f.execute { return; b = true }
    method()
    s.run()
    assertResult (false) (b)
  }

  it should "report an exception thrown from a task and continue" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    var a = false
    f.execute (throwDistinguishedException)
    f.execute (a = true)
    assertResult (false) (a)
    intercept [DistinguishedException] (s.run())
    s.run()
    assertResult (true) (a)
  }

  "Fiber.async" should "not invoke the callback" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    var a = false
    val cb = f.async [Unit] (cb => a = true) .capture()
    assertResult (false) (a)
    cb.assertNotInvoked()
    s.run()
    assertResult (true) (a)
    cb.assertNotInvoked()
  }

  it should "reject the return keyword" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    def method(): Async [Int] = f.async (_ => return null)
    method() .fail [ReturnException]
  }

  it should "report an exception through the callback" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    f.async [Unit] (cb => throw new DistinguishedException) .fail [DistinguishedException]
  }

  "Fiber.guard" should "report an exception through the callback" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    f.guard (throw new DistinguishedException) .fail [DistinguishedException]
  }

  it should "reject the return keyword" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    def method(): Async [Int] = f.guard (return supply (0))
    method() .fail [ReturnException]
  }

  "Fiber.supply" should "invoke the callback" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    var a = false
    f.supply (a = true) .pass
    assertResult (true) (a)
  }

  it should "reject the return keyword" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    def method(): Async [Int] = f.supply {return null}
    method() .fail [ReturnException]
  }

  it should "report an exception through the callback" in {
    implicit val s = StubScheduler.random()
    val f = new Fiber
    f.supply (throw new DistinguishedException) .fail [DistinguishedException]
  }}
