package com.treode.store.disk2

import java.util.ArrayList
import scala.collection.JavaConversions._
import com.treode.async.StubScheduler
import org.scalatest.FlatSpec

class DispatcherSpec extends FlatSpec {

  def list [M] (messages: M*): ArrayList [M] = {
    val a = new ArrayList [M]
    a.addAll (messages)
    a
  }

  class Receptor [M] extends (ArrayList [M] => Unit) {

    private var _invokation: Array [StackTraceElement] = null
    private var _messages: ArrayList [M] = null

    def wasInvoked: Boolean =
      _invokation != null

    private def assertNotInvoked() {
      if (!wasInvoked) {
        _invokation = Thread.currentThread.getStackTrace
      } else {
        val _second = Thread.currentThread.getStackTrace
        println ("First invokation:\n    " + (_invokation take (10) mkString "\n    "))
        println ("Second invokation:\n    " + (_second take (10) mkString "\n    "))
        assert (false, "Receiver was already invoked.")
      }}

    def apply (messages: ArrayList [M]): Unit = {
      assertNotInvoked()
      _messages = messages
    }

    def expect (messages: M*) {
      assert (wasInvoked, "Receiver was not invoked.")
      expectResult (list (messages: _*)) (_messages)
    }

    override def toString: String =
      if (!wasInvoked)
        "Receptor:NotInvoked"
      else
        s"Receptor:Passed(${_messages})"
  }

  "The Dispatcher" should "send one message to a later receiver" in {
    val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (scheduler)
    dsp.send (1)
    val rcpt = new Receptor [Int]
    dsp.receive (rcpt)
    scheduler.runTasks()
    rcpt.expect (1)
    dsp.replace (list())
  }

  it should "send multiple messages to a later receiver" in {
    val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (scheduler)
    dsp.send (1)
    dsp.send (2)
    dsp.send (3)
    val rcpt = new Receptor [Int]
    dsp.receive (rcpt)
    scheduler.runTasks()
    rcpt.expect (1, 2, 3)
    dsp.replace (list())
  }

  it should "send one message to an earlier receiver" in {
    val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (scheduler)
    val rcpt = new Receptor [Int]
    dsp.receive (rcpt)
    dsp.send (1)
    scheduler.runTasks()
    rcpt.expect (1)
    dsp.replace (list())
  }

  it should "send multiple messages to an earlier receiver" in {
    val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (scheduler)
    val rcpt1 = new Receptor [Int]
    dsp.receive (rcpt1)
    val rcpt2 = new Receptor [Int]
    dsp.receive (rcpt2)
    dsp.send (1)
    dsp.send (2)
    dsp.send (3)
    scheduler.runTasks()
    rcpt1.expect (1)
    assert (!rcpt2.wasInvoked)
    dsp.replace (list())
    scheduler.runTasks()
    rcpt2.expect (2, 3)
    dsp.replace (list())
  }

  it should "send multiple messages to a later receiver when disengaged" in {
    val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (scheduler)
    val rcpt1 = new Receptor [Int]
    dsp.receive (rcpt1)
    dsp.send (1)
    val rcpt2 = new Receptor [Int]
    dsp.receive (rcpt2)
    dsp.send (2)
    dsp.send (3)
    scheduler.runTasks()
    rcpt1.expect (1)
    assert (!rcpt2.wasInvoked)
    dsp.replace (list())
    scheduler.runTasks()
    rcpt2.expect (2, 3)
    dsp.replace (list())
  }

  it should "send rejects to the next receiver" in {
    val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (scheduler)
    dsp.send (1)
    dsp.send (2)
    val rcpt1 = new Receptor [Int]
    dsp.receive (rcpt1)
    scheduler.runTasks()
    rcpt1.expect (1, 2)
    dsp.replace (list (2))
    scheduler.runTasks()
    val rcpt2 = new Receptor [Int]
    dsp.receive (rcpt2)
    scheduler.runTasks()
    rcpt2.expect (2)
    dsp.replace (list())
  }

  it should "send rejects to the next receiver from earlier" in {
    val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (scheduler)
    dsp.send (1)
    dsp.send (2)
    val rcpt1 = new Receptor [Int]
    dsp.receive (rcpt1)
    val rcpt2 = new Receptor [Int]
    dsp.receive (rcpt2)
    scheduler.runTasks()
    rcpt1.expect (1, 2)
    assert (!rcpt2.wasInvoked)
    dsp.replace (list (2))
    scheduler.runTasks()
    rcpt2.expect (2)
    dsp.replace (list())
  }

  it should "send rejects and queued messages to the next receiver" in {
    val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (scheduler)
    dsp.send (1)
    dsp.send (2)
    val rcpt1 = new Receptor [Int]
    dsp.receive (rcpt1)
    dsp.send (3)
    scheduler.runTasks()
    rcpt1.expect (1, 2)
    dsp.replace (list (2))
    scheduler.runTasks()
    val rcpt2 = new Receptor [Int]
    dsp.receive (rcpt2)
    scheduler.runTasks()
    rcpt2.expect (2, 3)
    dsp.replace (list())
  }

  it should "send rejects and queued messages to the next receiver from earlier" in {
    val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (scheduler)
    dsp.send (1)
    dsp.send (2)
    val rcpt1 = new Receptor [Int]
    dsp.receive (rcpt1)
    val rcpt2 = new Receptor [Int]
    dsp.receive (rcpt2)
    dsp.send (3)
    scheduler.runTasks()
    rcpt1.expect (1, 2)
    assert (!rcpt2.wasInvoked)
    dsp.replace (list (2))
    scheduler.runTasks()
    rcpt2.expect (2, 3)
    dsp.replace (list())
  }}
