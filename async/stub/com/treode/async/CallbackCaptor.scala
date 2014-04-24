package com.treode.async

import java.util.concurrent.TimeoutException
import scala.util.{Failure, Success, Try}

import org.scalatest.Assertions

class CallbackCaptor [T] private extends (Try [T] => Unit) with Assertions {

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

  def apply (v: Try [T]): Unit = synchronized {
    invoked()
    v match {
      case Success (v) => _v = v
      case Failure (t) => _t = t
    }}

  def wasInvoked: Boolean = synchronized {
    _invokation != null
  }

  def hasPassed: Boolean = synchronized {
    _v != null
  }

  def hasFailed: Boolean = synchronized {
    _t != null
  }

  def hasTimedOut: Boolean = synchronized {
    _t != null && _t.isInstanceOf [TimeoutException]
  }

  def assertInvoked(): Unit = synchronized {
    assert (_invokation != null, "Expected callback to have been invoked, but it was not.")
  }

  def assertNotInvoked(): Unit = synchronized {
    if (_invokation != null)
      fail (
          "Expected callback to not have been invoked, but it was:\n" +
          (_invokation take (10) mkString "\n"))
  }

  def passed: T = synchronized {
    assertInvoked()
    if (_t != null)
      throw _t
    _v
  }

  def failed [E] (implicit m: Manifest [E]): E = synchronized {
    assertInvoked()
    if (_v != null)
      fail (
          "Expected operation to fail, but it passed:\n" +
          (_invokation take (10) mkString "\n"))
    assert (
        m.runtimeClass.isInstance (_t),
        s"Expected ${m.runtimeClass.getSimpleName}, found ${_t.getClass.getSimpleName}")
    _t.asInstanceOf [E]
  }

  override def toString: String = synchronized {
    if (_v != null)
      s"CallbackCaptor:Passed(${_v})"
    else if (_t != null)
      s"CallbackCaptor:Failed(${_t})"
    else
      s"CallbackCaptor:NotInvoked"
  }}

object CallbackCaptor {

  def apply [T] = new CallbackCaptor [T]
}
