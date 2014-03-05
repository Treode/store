package com.treode.async

import scala.util.Random

import Backoff.BackoffIterator

class Backoff private (
  random: Random,
  start: Int,
  jitter: Int,
  max: Int,
  retries: Int
) extends Iterable [Int] {

  require (start > 0 || jitter > 0, "Start or jitter must be greater than 0.")
  require (max > start + jitter, s"Max must be greater than start + jitter $max $start $jitter")
  require (retries >= 0, "Retries must be non-negative")

  def iterator: Iterator [Int] =
    new BackoffIterator (random, max, start + random.nextInt (jitter), retries)
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
        timeout = t + random.nextInt (t)
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
  ) (implicit
      random: Random
  ): Backoff =
    new Backoff (random, start, jitter, max, retries)
}
