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
    val s = StubScheduler.random()
    val f = new Fiber (s)
    var a = false
    f.execute (a = true)
    expectResult (false) (a)
    s.runTasks()
    expectResult (true) (a)
  }

  it should "run two queued tasks" in {
    val s = StubScheduler.random()
    val f = new Fiber (s)
    var a = false
    var b = false
    f.execute (a = true)
    f.execute (b = true)
    expectResult (false) (a)
    expectResult (false) (b)
    s.runTasks()
    expectResult (true) (a)
    expectResult (true) (b)
  }

  it should "run two tasks one after the other" in {
    val s = StubScheduler.random()
    val f = new Fiber (s)
    var a = false
    var b = false
    f.execute (a = true)
    expectResult (false) (a)
    s.runTasks()
    expectResult (true) (a)
    f.execute (b = true)
    expectResult (false) (b)
    s.runTasks()
    expectResult (true) (b)
  }

  it should "report an exception thrown from a task and continue" in {
    val s = StubScheduler.random()
    val f = new Fiber (s)
    var a = false
    f.execute (throwDistinguishedException)
    f.execute (a = true)
    expectResult (false) (a)
    intercept [DistinguishedException] (s.runTasks())
    s.runTasks()
    expectResult (true) (a)
  }

  "Fiber.async" should "not invoke the callback" in {
    val s = StubScheduler.random()
    val f = new Fiber (s)
    var a = false
    val cb = f.async [Unit] (cb => a = true) .capture()
    expectResult (false) (a)
    cb.expectNotInvoked()
    s.runTasks()
    expectResult (true) (a)
    cb.expectNotInvoked()
  }

  it should "report an exception through the callback" in {
    val s = StubScheduler.random()
    val f = new Fiber (s)
    val cb = f.async [Unit] (cb => throw new DistinguishedException) .capture()
    cb.expectNotInvoked()
    s.runTasks()
    cb.failed [DistinguishedException]
  }

  "Fiber.guard" should "report an exception through the callback" in {
    val s = StubScheduler.random()
    val f = new Fiber (s)
    val cb = f.guard (throw new DistinguishedException) .capture()
    cb.expectNotInvoked()
    s.runTasks()
    cb.failed [DistinguishedException]
  }

  "Fiber.run" should "invoke the callback" in {
    val s = StubScheduler.random()
    val f = new Fiber (s)
    var a = false
    val cb = CallbackCaptor [Unit]
    f.run (cb) (supply (a = true))
    expectResult (false) (a)
    cb.expectNotInvoked()
    s.runTasks()
    expectResult (true) (a)
    cb.expectInvoked()
  }

  it should "report an exception through the callback" in {
    val s = StubScheduler.random()
    val f = new Fiber (s)
    val cb = CallbackCaptor [Unit]
    f.run (cb) (throw new DistinguishedException)
    cb.expectNotInvoked()
    s.runTasks()
    cb.failed [DistinguishedException]
  }

  "Fiber.defer" should "not invoke the callback" in {
    val s = StubScheduler.random()
    val f = new Fiber (s)
    var a = false
    val cb = CallbackCaptor [Unit]
    f.defer (cb) (a = true)
    expectResult (false) (a)
    cb.expectNotInvoked()
    s.runTasks()
    expectResult (true) (a)
    cb.expectNotInvoked()
  }

  it should "report an exception through the callback" in {
    val s = StubScheduler.random()
    val f = new Fiber (s)
    val cb = CallbackCaptor [Unit]
    f.defer (cb) (throw new DistinguishedException)
    cb.expectNotInvoked()
    s.runTasks()
    cb.failed [DistinguishedException]
  }

  "Fiber.supply" should "invoke the callback" in {
    val s = StubScheduler.random()
    val f = new Fiber (s)
    var a = false
    val cb = f.supply (a = true) .capture()
    expectResult (false) (a)
    cb.expectNotInvoked()
    s.runTasks()
    expectResult (true) (a)
    cb.expectInvoked()
  }

  it should "report an exception through the callback" in {
    val s = StubScheduler.random()
    val f = new Fiber (s)
    val cb = f.supply (throw new DistinguishedException) .capture()
    cb.expectNotInvoked()
    s.runTasks()
    cb.failed [DistinguishedException]
  }}
