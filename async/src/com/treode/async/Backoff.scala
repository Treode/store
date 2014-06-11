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
  * This requires a [[scala.util.Random Random]] to form the iterator.  The PRNG is provided to
  * `iterator` rather than the constructor.  This allows you to define a `Backoff` as a constant
  * or configuration property.  You can use Scala's [[scala.util.Random Random]] in production,
  * and you can use [[com.treode.async.stubs.AsyncChecks AsyncChecks]] to seed a random in testing.
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
