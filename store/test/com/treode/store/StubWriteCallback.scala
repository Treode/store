package com.treode.store

private class StubWriteCallback extends WriteCallback {

  private def unexpected: Unit = throw new AssertionError ("Unexpected method call.")

  def pass (wt: TxClock) = unexpected
  def fail (t: Throwable) = throw t
  def collisions (ks: Set [Int]) = unexpected
  def advance() = unexpected
}
