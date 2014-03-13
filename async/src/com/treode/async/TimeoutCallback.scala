package com.treode.async

import java.util.concurrent.TimeoutException
import scala.util.{Failure, Random, Try}

import Scheduler.toRunnable

class TimeoutCallback [A] private [async] (
    fiber: Fiber,
    backoff: Backoff,
    rouse: => Any,
    private var cb: Callback [_]
) (
    implicit random: Random
) extends Callback [A] {

  private val iter = backoff.iterator

  if (iter.hasNext)
    fiber.delay (iter.next) (timeout())
  rouse

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

  def apply (v: Try [A]): Unit = synchronized {
    if (cb != null)
      _apply (v)
  }}
