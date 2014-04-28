package com.treode.async

import com.treode.async.implicits._

import Async.async

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
