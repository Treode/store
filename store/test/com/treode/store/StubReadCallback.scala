package com.treode.store

class StubReadCallback extends ReadCallback {

  private def unexpected: Unit = throw new AssertionError ("Unexpected method call.")

  def pass (vs: Seq [Value]) = unexpected
  def fail (t: Throwable) = throw t
}
