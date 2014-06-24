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

package com.treode.async

import java.util.concurrent.TimeoutException
import scala.util.{Failure, Random, Try}

import Scheduler.toRunnable

/** Experimental. */
class TimeoutCallback [A] private [async] (
    fiber: Fiber,
    backoff: Backoff,
    _rouse: => Any,
    private var cb: Callback [_]
) (
    implicit random: Random
) extends Callback [A] {

  private val iter = backoff.iterator

  if (iter.hasNext)
    fiber.delay (iter.next) (timeout())

  private def _apply (v: Try [A]) {
    val _cb = cb .asInstanceOf [Callback [A]]
    cb = null
    _cb (v)
  }

  private def timeout() {
    if (cb != null) {
      if (iter.hasNext) {
        try {
          fiber.delay (iter.next) (timeout())
          rouse
        } catch {
          case t: Throwable => _apply (Failure (t))
        }
      } else {
        _apply (Failure (new TimeoutException))
      }}}

  def invoked: Boolean =
    cb == null

  def rouse(): Unit =
    fiber.execute (_rouse)

  def apply (v: Try [A]): Unit = fiber.execute {
    if (cb != null)
      _apply (v)
  }}
