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

import scala.util.{Failure, Success, Try}

import com.treode.async.implicits._

private abstract class AbstractLatch [A, B] (cb: Callback [B])
extends Callback [A] {

  private var started = false
  private var count = 0
  private var thrown = List.empty [Throwable]

  /** Invoked inside `synchronized` after last release. */
  protected def result: B

  /** Invoked inside `synchronized` in response to `apply`. */
  protected def add (v: A)

  private [this] def finish() {
    if (!thrown.isEmpty)
      cb.fail (MultiException.fit (thrown))
    else
      cb.pass (result)
  }

  private [this] def release() {
    require (!started || count > 0, "Latch was already released.")
    count -= 1
    if (!started || count > 0)
      return
    finish()
  }

  def start (count: Int): Unit = synchronized {
    require (!started)
    this.started = true
    this.count += count
    if (this.count > 0)
      return
    finish()
  }

  def apply (v: Try [A]): Unit = synchronized {
    v match {
      case Success (v) =>
        add (v)
        release()
      case Failure (t) =>
        thrown ::= t
        release()
    }}}
