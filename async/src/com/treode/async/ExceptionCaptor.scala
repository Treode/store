package com.treode.async

import scala.util.Try

private class ExceptionCaptor [A] (cb: Callback [A]) extends Callback [A] {

  private var t: Throwable = null

  def apply (v: Try [A]): Unit =
    try {
      cb (v)
    } catch {
      case _t: Throwable => t = _t
    }

  def get: Unit =
    if (t != null)
      throw t
}
