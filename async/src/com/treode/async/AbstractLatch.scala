package com.treode.async

private abstract class AbstractLatch [A] (private var count: Int, cb: Callback [A]) {

  private var thrown = List.empty [Throwable]

  def init() {
    if (count == 0)
      cb.pass (value)
  }

  def release() {
    require (count > 0, "Latch was already released.")
    count -= 1
    if (count > 0)
      return
    if (!thrown.isEmpty)
      cb.fail (MultiException.fit (thrown))
    else
      cb.pass (value)
  }

  def fail (t: Throwable): Unit = synchronized {
    thrown ::= t
    release()
  }

  def value: A
}
