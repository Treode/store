package com.treode.cluster.misc

import scala.util.Random

private class BackoffIterator (
  private val random: Random,
  private val max: Int,
  private var delay: Int,
  private var timeout: Int,
  private var retries: Int) extends Iterator [Int] {

  def hasNext: Boolean = retries > 0

  def next: Int = {
    if (delay > 0) {
      val t = delay
      delay = 0
      t
    } else {
      val t = timeout
      if (t < max) {
        timeout = t + random.nextInt (t)
        if (timeout > max) timeout = max
      }
      retries -= 1
      t
    }}}

class BackoffTimer private (
  random: Random,
  start: Int,
  jitter: Int,
  max: Int,
  retries: Int,
  delay: Int)
    extends Iterable [Int] {

  require (start > 0 || jitter > 0, "Start or jitter must be greater than 0.")
  require (max > start + jitter, "Max must be greater than start + jitter")
  require (retries >= 0, "Retries must be non-negative")

  def iterator: Iterator [Int] =
    new BackoffIterator (random, max, delay, start + random.nextInt (jitter), retries)
}

object BackoffTimer {

  def apply (
    start: Int,
    jitter: Int,
    max: Int = Int.MaxValue,
    retries: Int = Int.MaxValue,
    delay: Int = 0)
        (implicit random: Random) =
    new BackoffTimer (random, start, jitter, max, retries, delay)
}
