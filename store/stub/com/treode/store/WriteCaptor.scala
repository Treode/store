package com.treode.store

import java.util.concurrent.TimeoutException

class WriteCaptor extends WriteCallback {

  private var _invokation: Array [StackTraceElement] = null
  private var _v: TxClock = TxClock.zero
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
      assert (false, "WriteCallback was already invoked.")
    }}

  private def assertInvoked (e: Boolean) {
    if (e && _t != null)
      throw new AssertionError (_t)
    assert (wasInvoked, "WriteCallback was not invoked.")
  }

  def pass (v: TxClock) {
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
    _v != TxClock.zero

  def passed: TxClock = {
    assertInvoked (true)
    if (_t != null)
      throw new AssertionError (_t)
    assert (hasPassed, "WriteCallback did not pass.")
    _v
  }

  def hasFailed: Boolean =
    _t != null

  def hasTimedOut: Boolean =
    _t != null && _t.isInstanceOf [TimeoutException]

  def failed: Throwable = {
    assertInvoked (false)
    assert (hasFailed, "WriteCallback did not fail.")
    _t
  }

  def hasCollided: Boolean =
    _ks != null

  def collided: Set [Int] = {
    assertInvoked (true)
    assert (hasCollided, "WriteCallback did not collide.")
    _ks
  }

  def hasAdvanced: Boolean =
    _advance

  def advanced: Boolean = {
    assertInvoked (true)
    assert (hasAdvanced, "WriteCallback did not advance.")
    _advance
  }

  override def toString: String =
    if (!wasInvoked)
      "WriteCaptor:NotInvoked"
    else if (hasPassed)
      s"WriteCaptor:Passed(${_v})"
    else if (hasFailed)
      s"WriteCaptor:Failed(${_t})"
    else if (hasCollided)
      s"WriteCaptor:Collided(${_ks})"
    else if (hasAdvanced)
      "WriteCaptor:Advanced"
    else
      "WriteCaptor:Confused"
}
