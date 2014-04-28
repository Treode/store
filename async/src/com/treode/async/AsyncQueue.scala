package com.treode.async

import scala.runtime.NonLocalReturnControl

import com.treode.async._
import com.treode.async.implicits._

import Callback.ignore

class AsyncQueue (fiber: Fiber) (deque: => Option [Runnable]) {

  private [this] var _engaged = true

  private def _reengage() {
    deque match {
      case Some (task) =>
        _engaged = true
        task.run()
      case None =>
        _engaged = false
    }}

  private def reengage(): Unit =
    fiber.execute {
      _reengage()
    }

  def engaged: Boolean = _engaged

  def run [A] (cb: Callback [A]) (task: => Async [A]): Option [Runnable] =
    Some (new Runnable {
      def run(): Unit = Async.guard (task) ensure (reengage()) run (cb)
    })

  def launch(): Unit =
    reengage()

  def execute (f: => Any): Unit =
    fiber.execute {
      f
      if (!_engaged)
        _reengage()
    }

  def async [A] (f: Callback [A] => Any): Async [A] =
    fiber.async  { cb =>
      f (cb)
      if (!_engaged)
        _reengage()
    }}

object AsyncQueue {

  def apply (fiber: Fiber) (deque: => Option [Runnable]): AsyncQueue =
    new AsyncQueue (fiber) (deque)
}
