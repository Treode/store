package com.treode.concurrent

import org.scalatest.FlatSpec

class FiberSpec extends FlatSpec {

  class DistinguishedException extends Exception

  val throwDistinguishedException =
    new Runnable {
      def run() = throw new DistinguishedException
    }

  "A Fiber" should "run one task" in {
    val s = new StubScheduler
    val f = new Fiber (s)
    var a = false
    f.execute (a = true)
    expectResult (false) (a)
    s.nextTask()
    expectResult (true) (a)
  }

  it should "run two queued tasks" in {
    val s = new StubScheduler
    val f = new Fiber (s)
    var a = false
    var b = false
    f.execute (a = true)
    f.execute (b = true)
    expectResult (false) (a)
    expectResult (false) (b)
    s.nextTask()
    expectResult (true) (a)
    expectResult (false) (b)
    s.nextTask()
    expectResult (true) (b)
  }

  it should "run two tasks one after the other" in {
    val s = new StubScheduler
    val f = new Fiber (s)
    var a = false
    var b = false
    f.execute (a = true)
    expectResult (false) (a)
    s.nextTask()
    expectResult (true) (a)
    f.execute (b = true)
    expectResult (false) (b)
    s.nextTask()
    expectResult (true) (b)
  }

  it should "report an exception thrown from a task and continue" in {
    val s = new StubScheduler
    val f = new Fiber (s)
    var a = false
    f.execute (throwDistinguishedException)
    f.execute (a = true)
    expectResult (false) (a)
    intercept [DistinguishedException] (s.nextTask())
    expectResult (false) (a)
    s.nextTask()
    expectResult (true) (a)
  }

  it should "run a suspendable task" in {
    val s = new StubScheduler
    val f = new Fiber (s)
    var a = false
    f.begin {cb => a = true; cb()}
    expectResult (false) (a)
    s.nextTask()
    expectResult (true) (a)
  }

  it should "report an exception thrown from a suspendable task and continue" in {
    val s = new StubScheduler
    val f = new Fiber (s)
    var a = false
    f.begin {cb => throw new DistinguishedException}
    f.execute (a = true)
    expectResult (false) (a)
    intercept [DistinguishedException] (s.nextTask())
    expectResult (false) (a)
    s.nextTask()
    expectResult (true) (a)
  }

  it should "report an exception passed from a suspendable task and continue" in {
    val s = new StubScheduler
    val f = new Fiber (s)
    var a = false
    f.begin {cb => cb.fail (new DistinguishedException)}
    f.execute (a = true)
    expectResult (false) (a)
    intercept [DistinguishedException] (s.nextTask())
    expectResult (false) (a)
    s.nextTask()
    expectResult (true) (a)
  }}
