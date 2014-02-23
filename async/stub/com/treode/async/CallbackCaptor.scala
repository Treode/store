package com.treode.async

import java.util.concurrent.TimeoutException
import org.scalatest.Assertions

class CallbackCaptor [T] protected extends Callback [T] {

  private var _invokation: Array [StackTraceElement] = null
  private var _v: T = null.asInstanceOf [T]
  private var _t: Throwable = null

  private def invoked() {
    if (_invokation == null) {
      _invokation = Thread.currentThread.getStackTrace
    } else {
      val _second = Thread.currentThread.getStackTrace
      println ("First invokation:\n    " + (_invokation take (10) mkString "\n    "))
      println ("Second invokation:\n    " + (_second take (10) mkString "\n    "))
      assert (false, "Callback was already invoked.")
    }}

  def pass (v: T) = {
    invoked()
    _v = v
  }

  def fail (t: Throwable) {
    invoked()
    _t = t
  }

  def wasInvoked: Boolean =
    _invokation != null

  def hasPassed: Boolean =
    _v != null

  def hasFailed: Boolean =
    _t != null

  def hasTimedOut: Boolean =
    _t != null && _t.isInstanceOf [TimeoutException]

  def expectInvoked(): Unit =
    assert (_invokation != null, "Expected callback to have been invoked, but it was not.")

  def expectNotInvoked() {
    if (_invokation != null)
      Assertions.fail (
          "Expected callback to not have been invoked, but it was:\n" +
          (_invokation take (10) mkString "\n"))
  }

  def passed: T = {
    expectInvoked()
    if (_t != null)
      throw _t
    _v
  }

  def failed [E] (implicit m: Manifest [E]): E = {
    expectInvoked()
    if (_v != null)
      Assertions.fail (
          "Expected operation to fail, but it passed:\n" +
          (_invokation take (10) mkString "\n"))
    Assertions.assert (
        m.runtimeClass.isInstance (_t),
        s"Expected ${m.runtimeClass.getSimpleName}, found ${_t.getClass.getSimpleName}")
    _t.asInstanceOf [E]
  }

  override def toString: String =
    if (_v != null)
      s"CallbackCaptor:Passed(${_v})"
    else if (_t != null)
      s"CallbackCaptor:Failed(${_t})"
    else
      s"CallbackCaptor:NotInvoked"
}

object CallbackCaptor {

  def apply [T] = new CallbackCaptor [T]
}
