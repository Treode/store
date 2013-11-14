package com.treode.store

private class StubPrepareCallback extends PrepareCallback {

  private def unexpected: Unit = throw new AssertionError ("Unexpected method call.")

  def pass (tx: Transaction) = unexpected
  def fail (t: Throwable) = throw t
  def collisions (ks: Set [Int]) = unexpected
  def advance() = unexpected
}
