package com.treode.store

class StubWriteCallback extends WriteCallback {

  private def unexpected: Unit = throw new AssertionError ("Unexpected method call.")

  def pass (tx: Transaction) = unexpected
  def fail (t: Throwable) = throw t
  def advance() = unexpected
  def collisions (ks: Set [Int]) = unexpected
}
