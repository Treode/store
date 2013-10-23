package com.treode.store.local

import com.treode.store.{Transaction, WriteCallback}

class StubWriteCallback extends WriteCallback {

  private def unexpected: Unit = throw new AssertionError ("Unexpected method call.")

  def pass (tx: Transaction) = unexpected
  def fail (t: Throwable) = unexpected
  def advance() = unexpected
  def conflicts (ks: Set [Int]) = unexpected
}
