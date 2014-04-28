package com.treode.disk

import scala.collection.mutable.UnrolledBuffer
import scala.util.Random

import com.treode.async.stubs.StubScheduler
import org.scalatest.FlatSpec

import DispatcherTestTools._

class DispatcherSpec extends FlatSpec {

  "The Dispatcher" should "send one message to a later receiver" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (0)
    dsp.send (1)
    dsp.expect (1)
    dsp.replace (list())
    dsp.expectNone()
  }

  it should "send multiple messages to a later receiver" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (0)
    dsp.send (1)
    dsp.send (2)
    dsp.send (3)
    dsp.expect (1, 2, 3)
    dsp.replace (list())
    dsp.expectNone()
  }

  it should "send one message to an earlier receiver" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (0)
    val rcpt = dsp.receptor()
    dsp.send (1)
    rcpt.expect (1)
    dsp.replace (list())
    dsp.expectNone()
  }

  it should "send multiple messages to an earlier receiver" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (0)
    val rcpt1 = dsp.receptor()
    val rcpt2 = dsp.receptor()
    dsp.send (1)
    dsp.send (2)
    dsp.send (3)
    rcpt1.expect (1)
    rcpt2.assertNotInvoked()
    dsp.replace (list())
    rcpt2.expect (2, 3)
    dsp.replace (list())
    dsp.expectNone()
  }

  it should "send multiple messages to a later receiver when disengaged" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (0)
    val rcpt1 = dsp.receptor()
    dsp.send (1)
    val rcpt2 = dsp.receptor()
    dsp.send (2)
    dsp.send (3)
    rcpt1.expect (1)
    rcpt2.assertNotInvoked()
    dsp.replace (list())
    rcpt2.expect (2, 3)
    dsp.replace (list())
    dsp.expectNone()
  }

  it should "send rejects to the next receiver" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (0)
    dsp.send (1)
    dsp.send (2)
    dsp.expect (1, 2)
    dsp.replace (list (2))
    dsp.expect (2)
    dsp.replace (list())
  }

  it should "send rejects to the next receiver from earlier" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (0)
    dsp.send (1)
    dsp.send (2)
    val rcpt1 = dsp.receptor()
    val rcpt2 = dsp.receptor()
    rcpt1.expect (1, 2)
    rcpt2.assertNotInvoked()
    dsp.replace (list (2))
    rcpt2.expect (2)
    dsp.replace (list())
  }

  it should "send rejects and queued messages to the next receiver" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (0)
    dsp.send (1)
    dsp.send (2)
    val rcpt1 = dsp.receptor()
    dsp.send (3)
    rcpt1.expect (1, 2)
    dsp.replace (list (2))
    scheduler.runTasks()
    val rcpt2 = dsp.receptor()
    rcpt2.expect (2, 3)
    dsp.replace (list())
  }

  it should "send rejects and queued messages to the next receiver from earlier" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (0)
    dsp.send (1)
    dsp.send (2)
    val rcpt1 = dsp.receptor()
    val rcpt2 = dsp.receptor()
    dsp.send (3)
    rcpt1.expect (1, 2)
    rcpt2.assertNotInvoked()
    dsp.replace (list (2))
    rcpt2.expect (2, 3)
    dsp.replace (list())
  }

  it should "recycle rejects until accepted" in {

    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (0)

    var received = Set.empty [Int]

    /* Accept upto 5 messages m where (m % n == i).
     * Ensure m was not already accepted.
     */
    def receive (n: Int, i: Int) (num: Long, msgs: UnrolledBuffer [Int]) {
      assert (!(msgs exists (received contains _)))
      val (accepts, rejects) = msgs.partition (_ % n == i)
      dsp.replace ((accepts drop 5) ++ rejects)
      received ++= accepts take 5
      dsp.receive (receive (n, i))
    }

    dsp.receive (receive (3, 0))
    dsp.receive (receive (3, 1))
    dsp.receive (receive (3, 2))

    val count = 100
    for (i <- 0 until count)
      dsp.send (i)
    scheduler.runTasks()

    assertResult ((0 until count).toSet) (received)
  }}
