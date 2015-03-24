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

import scala.util.Random

import Backoff.BackoffIterator

/** A series of ints that grows almost exponentially.
  *
  * This provides a sequence
  * ''i'',,''n'',, = ''i'',,''n''-1,, + `random` (''i'',,''n''-1,,).
  * ''i'',,0,, = `start` + `random` (`jitter`).
  * ''i'',,''n'',, never exceeds `max`.
  * The sequence has length `retries`.
  *
  * This requires a [[$Random Random]] to form the iterator.  The PRNG is provided to `iterator`
  * rather than the constructor.  This allows you to define a `Backoff` as a constant or
  * configuration property.
  *
  * @define Random http://www.scala-lang.org/api/current/index.html#scala.util.Random
  */
class Backoff private (
    start: Int,
    jitter: Int,
    max: Int,
    retries: Int
) {

  require (start > 0 || jitter > 0, "Start or jitter must be greater than 0.")
  require (max > start + jitter, "Max must be greater than start + jitter")
  require (retries >= 0, "Retries must be non-negative")

  def iterator (implicit random: Random): Iterator [Int] =
    new BackoffIterator (random, max, start + random.nextInt (jitter), retries)

  override def toString: String =
    s"Backoff($start, $jitter, $max, $retries)"
}

object Backoff {

  private class BackoffIterator (
      private val random: Random,
      private val max: Int,
      private var timeout: Int,
      private var retries: Int
  ) extends Iterator [Int] {

    def hasNext: Boolean = retries > 0

    def next: Int = {
      val t = timeout
      if (t < max) {
        timeout = t + 1 + random.nextInt (t)
        if (timeout > max)
          timeout = max
      }
      retries -= 1
      t
    }}

  def apply (
    start: Int,
    jitter: Int,
    max: Int = Int.MaxValue,
    retries: Int = Int.MaxValue
  ): Backoff =
    new Backoff (start, jitter, max, retries)
}
