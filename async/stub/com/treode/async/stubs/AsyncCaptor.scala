package com.treode.async.stubs

import com.treode.async.{Async, Callback}
import com.treode.async.implicits._

import Async.async

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
class AsyncCaptor [A] private {

  private var cb: Callback [A] = null

  def start(): Async [A] =
    async { cb =>
      require (this.cb == null, "An async is already outstanding.")
      this.cb = cb
    }

  def pass (v: A) {
    require (this.cb != null)
    val cb = this.cb
    this.cb = null
    cb.pass (v)
  }

  def fail (t: Throwable) {
    require (this.cb != null)
    val cb = this.cb
    this.cb = null
    cb.fail (t)
  }}

object AsyncCaptor {

  def apply [A]: AsyncCaptor [A] = new AsyncCaptor [A]
}
