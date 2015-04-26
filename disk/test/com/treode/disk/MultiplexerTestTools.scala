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

package com.treode.disk

import scala.collection.mutable.UnrolledBuffer
import scala.reflect.ClassTag

import com.treode.async.stubs.StubScheduler
import org.scalatest.Assertions

import Assertions.{assertResult, fail}

object MultiplexerTestTools {

  def list [M] (messages: M*) (implicit mtag: ClassTag [M]): UnrolledBuffer [M] =
    UnrolledBuffer [M] (messages: _*)

  class Receptor [M] (implicit mtag: ClassTag [M])
  extends ((Long, UnrolledBuffer [M]) => Unit) {

    private var _invokation: Array [StackTraceElement] = null
    private var _messages: UnrolledBuffer [M] = null

    private def _invoked() {
      if (_invokation == null) {
        _invokation = Thread.currentThread.getStackTrace
      } else {
        val _second = Thread.currentThread.getStackTrace
        println ("First invokation:\n    " + (_invokation take (10) mkString "\n    "))
        println ("Second invokation:\n    " + (_second take (10) mkString "\n    "))
        assert (false, "DispatchReceptor was already invoked.")
      }}

    def apply (batch: Long, messages: UnrolledBuffer [M]): Unit = {
      _invoked()
      _messages = messages
    }

    def assertNotInvoked() {
      if (_invokation != null)
        fail (
            "Expected callback to not have been invoked, but it was:\n" +
            (_invokation take (10) mkString "\n"))
    }

    def expect (messages: M*) (implicit scheduler: StubScheduler) {
      scheduler.run()
      assert (_invokation != null, "Receiver was not invoked.")
      assertResult (list (messages: _*)) (_messages)
    }

    def expectNone () (implicit scheduler: StubScheduler) {
      scheduler.run()
      assertNotInvoked()
    }

    override def toString: String =
      if (_invokation == null)
        "DispatchReceptor:NotInvoked"
      else
        s"DispatchReceptor:Passed(${_messages})"
  }

  implicit class RichDispatcher [M] (dsp: Dispatcher [M]) (implicit mtag: ClassTag [M]) {

    def receptor(): Receptor [M] = {
      val rcpt = new Receptor [M]
      dsp.receive (rcpt)
      rcpt
    }

    def expect (messages: M*) (implicit scheduler: StubScheduler) {
      val rcpt = receptor()
      rcpt.expect (messages: _*)
    }

    def expectNone () (implicit scheduler: StubScheduler) {
      val rcpt = receptor()
      rcpt.expectNone()
    }}

  implicit class RichMultiplexer [M] (mplx: Multiplexer [M]) (implicit mtag: ClassTag [M]) {

    def receptor(): Receptor [M] = {
      val rcpt = new Receptor [M]
      mplx.receive (rcpt)
      rcpt
    }

    def expect (messages: M*) (implicit scheduler: StubScheduler) {
      val rcpt = receptor()
      rcpt.expect (messages: _*)
    }

    def expectNone () (implicit scheduler: StubScheduler) {
      val rcpt = receptor()
      rcpt.expectNone()
    }}}
