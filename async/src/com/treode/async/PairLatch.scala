package com.treode.async

private class PairLatch [A, B] (cb: Callback [(A, B)])
extends AbstractLatch [(A, B)] (2, cb) {

  private var va: A = null.asInstanceOf [A]
  private var vb: B = null.asInstanceOf [B]

  def value: (A, B) = (va, vb)

  init()

  val cbA = new Callback [A] {
    def pass (v: A) {
      require (va == null, "Value 'a' was already set.")
      va = v
      release()
    }
    def fail (t: Throwable): Unit =
      PairLatch.this.fail (t)
  }

  val cbB = new Callback [B] {
    def pass (v: B) {
      require (vb == null, "Value 'b' was already set.")
      vb = v
      release()
    }
    def fail (t: Throwable): Unit =
      PairLatch.this.fail (t)
  }}
