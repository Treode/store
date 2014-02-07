package com.treode.async

import java.util.concurrent.TimeoutException

class CallbackCaptor [T] extends Callback [T] {

  private var _invokation: Array [StackTraceElement] = null
  private var _v: T = null.asInstanceOf [T]
  private var _t: Throwable = null

  def wasInvoked: Boolean =
    _invokation != null

  private def assertNotInvoked() {
    if (!wasInvoked) {
      _invokation = Thread.currentThread.getStackTrace
    } else {
      val _second = Thread.currentThread.getStackTrace
      println ("First invokation:\n    " + (_invokation take (10) mkString "\n    "))
      println ("Second invokation:\n    " + (_second take (10) mkString "\n    "))
      assert (false, "Callback was already invoked.")
    }}

  private def assertInvoked (e: Boolean) {
    if (e && _t != null)
      throw new AssertionError (_t)
    assert (wasInvoked, "Expected callback to have been invoked, but it was not.")
  }

  def pass (v: T) = {
    assertNotInvoked()
    _v = v
  }

  def fail (t: Throwable) {
    assertNotInvoked()
    _t = t
  }

  def hasPassed: Boolean =
    _v != null

  def passed: T = {
    assertInvoked (true)
    assert (hasPassed, "Expected operation to pass, but it failed.")
    _v
  }

  def hasFailed: Boolean =
    _t != null

  def hasTimedOut: Boolean =
    _t != null && _t.isInstanceOf [TimeoutException]

  def failed [E] (implicit m: Manifest [E]): E = {
    assertInvoked (false)
    assert (hasFailed, "Expected operation to fail, but it passed.")
    assert (
        m.runtimeClass.isInstance (_t),
        s"Expected ${m.runtimeClass.getSimpleName}, found ${_t.getClass.getSimpleName}")
    _t.asInstanceOf [E]
  }

  override def toString: String =
    if (!wasInvoked)
      "CallbackCaptor:NotInvoked"
    else if (hasPassed)
      s"CallbackCaptor:Passed(${_v})"
    else if (hasFailed)
      s"CallbackCaptor:Failed(${_t})"
    else
      "CallbackCaptor:Confused"
}
