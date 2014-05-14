package com.treode.async.stubs

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.async.implicits._

import Async.async
import Callback.fanout

/** Capture an asynchronous call so it may be completed later.
  *
  * You may `start` an asynchronous operation once; if you are mocking a method that is called
  * multiple times, return a new `AsyncCaptor` for each call.  You may not call `pass` or `fail`
  * until after  `start`.
  *
  * This class is most easily used with the single-threaded
  * [[com.treode.async.stubs.StubScheduler StubScheduler]].  If you are using a multithreaded
  * scheduler, you must arrange that `start` happens-before `pass` or `fail`.
  */
class AsyncCaptor [A] private (implicit scheduler: Scheduler) {

  private var cbs = List.empty [Callback [A]]

  def start(): Async [A] =
    async { cb =>
      require (cbs.isEmpty, "An async is already outstanding.")
      cbs ::= cb
    }

  def add(): Async [A] =
    async { cb =>
      cbs ::= cb
    }

  def pass (v: A) {
    require (!cbs.isEmpty)
    val cb = fanout (cbs)
    cbs = List.empty
    cb.pass (v)
  }

  def fail (t: Throwable) {
    require (!cbs.isEmpty)
    val cb = fanout (cbs)
    cbs = List.empty
    cb.fail (t)
  }}

object AsyncCaptor {

  def apply [A] (implicit scheduler: Scheduler): AsyncCaptor [A] =
    new AsyncCaptor [A]
}
