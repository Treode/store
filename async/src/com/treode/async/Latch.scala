package com.treode.async

private class Latch (private var count: Int, cb: Callback [Unit]) extends Callback [Any] {

  private var thrown = List.empty [Throwable]

  if (count == 0)
    cb()

  private def release() {
    require (count > 0, "Latch was already released.")
    count -= 1
    if (count > 0)
      return
    if (!thrown.isEmpty)
      cb.fail (MultiException (thrown))
    else
      cb()
  }

  def pass (v: Any): Unit = synchronized {
    release()
  }

  def fail (t: Throwable): Unit = synchronized {
    thrown ::= t
    release()
  }}
