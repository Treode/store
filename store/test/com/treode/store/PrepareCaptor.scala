package com.treode.store

import java.util.concurrent.TimeoutException

private class PrepareCaptor extends PrepareCallback {

  private var _invokation: Array [StackTraceElement] = null
  private var _v: Transaction = null
  private var _t: Throwable = null
  private var _ks: Set [Int] = null
  private var _advance = false

  private def wasInvoked: Boolean =
    _invokation != null

  private def assertNotInvoked() {
    if (!wasInvoked) {
      _invokation = Thread.currentThread.getStackTrace
    } else {
      val _second = Thread.currentThread.getStackTrace
      println ("First invokation:\n    " + (_invokation take (10) mkString "\n    "))
      println ("Second invokation:\n    " + (_second take (10) mkString "\n    "))
      assert (false, "PrepareCallback was already invoked.")
    }}

  private def assertInvoked (e: Boolean) {
    if (e && _t != null)
      throw new AssertionError (_t)
    assert (wasInvoked, "PrepareCallback was not invoked.")
  }

  def pass (v: Transaction) {
    assertNotInvoked()
    _v = v
  }

  def fail (t: Throwable) {
    assertNotInvoked()
    _t = t
  }

  def collisions (ks: Set [Int]) {
    assertNotInvoked()
    _ks = ks
  }

  def advance() {
    assertNotInvoked()
    _advance = true
  }

  def hasPassed: Boolean =
    _v != null

  def passed: Transaction = {
    assertInvoked (true)
    if (_t != null)
      throw new AssertionError (_t)
    assert (hasPassed, "PrepareCallback did not pass.")
    _v
  }

  def hasFailed: Boolean =
    _t != null

  def hasTimedOut: Boolean =
    _t != null && _t.isInstanceOf [TimeoutException]

  def failed: Throwable = {
    assertInvoked (false)
    assert (hasFailed, "PrepareCallback did not fail.")
    _t
  }

  def hasCollided: Boolean =
    _ks != null

  def collided: Set [Int] = {
    assertInvoked (true)
    assert (hasCollided, "PrepareCallback did not collide.")
    _ks
  }

  def hasAdvanced: Boolean =
    _advance

  def advanced: Boolean = {
    assertInvoked (true)
    assert (hasAdvanced, "PrepareCallback did not advance.")
    _advance
  }

  override def toString: String =
    if (!wasInvoked)
      "PrepareCaptor:NotInvoked"
    else if (hasPassed)
      s"PrepareCaptor:Passed(${_v})"
    else if (hasFailed)
      s"PrepareCaptor:Failed(${_t})"
    else if (hasCollided)
      s"PrepareCaptor:Collided(${_ks})"
    else if (hasAdvanced)
      "PrepareCaptor:Advanced"
    else
      "PrepareCaptor:Confused"
}
