package com.treode.concurrent

class CallbackCaptor [T] extends Callback [T] {

  private var _invokation: Array [StackTraceElement] = null
  private var _v: T = null.asInstanceOf [T]
  private var _t: Throwable = null

  private def notInvoked() {
    if (_invokation == null) {
      _invokation = Thread.currentThread.getStackTrace
    } else {
      val _second = Thread.currentThread.getStackTrace
      println ("First invokation:\n    " + (_invokation take (10) mkString "\n    "))
      println ("Second invokation:\n    " + (_second take (10) mkString "\n    "))
      assert (false, "Callback was already invoked.")
    }}

  def pass (v: T) = {
    notInvoked()
    _v = v
  }

  def fail (t: Throwable) {
    notInvoked()
    _t = t
  }

  private def invoked() {
    assert (_invokation != null, "Callback was not invoked.")
  }

  def passed: T = {
    invoked()
    assert (_v != null, "Operation failed.")
    _v
  }

  def failed: Throwable = {
    invoked()
    assert (_t != null, "Operation passed.")
    _t
  }}
