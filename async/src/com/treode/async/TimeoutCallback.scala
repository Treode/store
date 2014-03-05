package com.treode.async

import java.util.concurrent.TimeoutException
import scala.util.Random

import Scheduler.toRunnable

class TimeoutCallback [-A] private [async] (
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

  private def _pass (v: A) {
    val _cb = cb
    cb = null
    _cb .asInstanceOf [Callback [A]] .pass (v)
  }

  private def _fail (t: Throwable) {
    val _cb = cb
    cb = null
    _cb.fail (t)
  }

  private def timeout() {
    if (cb != null) {
      if (iter.hasNext) {
        try {
          fiber.delay (iter.next) (timeout())
          rouse
        } catch {
          case t: Throwable => _fail (t)
        }
      } else {
        _fail (new TimeoutException)
      }}}

  def invoked: Boolean =
    cb == null

  def pass (v: A): Unit =
    if (cb != null)
      _pass (v)

  def fail (t: Throwable): Unit =
    if (cb != null)
      _fail (t)
}
