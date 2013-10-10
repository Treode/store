package com.treode.store.local

import com.treode.store.{ReadCallback, Value}

class StubReadCallback extends ReadCallback {

  private def unexpected: Unit = throw new AssertionError ("Unexpected method call.")

  def apply (vs: Seq [Value]) = unexpected
  def fail (t: Throwable) = unexpected
}
