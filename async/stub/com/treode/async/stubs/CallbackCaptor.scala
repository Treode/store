/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.async.stubs

import scala.util.{Failure, Success, Try}

import org.scalatest.Assertions

/** Capture the result of an asynchronous call so you may test for success or failure later. */
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

  /** True if the callback was invoked, regardless of [[scala.util.Success Success]] or
   *  [[scala.util.Failure Failure]].
   */
  def wasInvoked: Boolean = synchronized {
    _invokation != null
  }

  /** True if the callback was invoked with [[scala.util.Success Success]]. */
  def hasPassed: Boolean = synchronized {
    _v != null
  }

  /** True if the callback was invoked with [[scala.util.Failure Failure]]. */
  def hasFailed [A] (implicit m: Manifest [A]): Boolean = synchronized {
    _t != null && m.runtimeClass.isInstance (_t)
  }

  /** Throw a testing error if the callback was not invoked. */
  def assertInvoked(): Unit = synchronized {
    assert (_invokation != null, "Expected callback to have been invoked, but it was not.")
  }

  /** Throw a testing error if the callback was invoked. */
  def assertNotInvoked(): Unit = synchronized {
    if (_invokation != null)
      fail (
          "Expected callback to not have been invoked, but it was:\n" +
          (_invokation take (10) mkString "\n"))
  }

  /** Assert the callback was invoked with [[scala.util.Success Success]] and return the result. */
  def passed: T = synchronized {
    assertInvoked()
    if (_t != null)
      throw _t
    _v
  }

  /** Assert the callback was invoked with [[scala.util.Failure Failure]] and return the
    * exception.
    */
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
