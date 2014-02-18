package com.treode.async

private class TripleLatch [A, B, C] (cb: Callback [(A, B, C)])
extends AbstractLatch [(A, B, C)] (3, cb) {

  private var va: A = null.asInstanceOf [A]
  private var vb: B = null.asInstanceOf [B]
  private var vc: C = null.asInstanceOf [C]

  def value: (A, B, C) = (va, vb, vc)

  init()

  val cbA = new Callback [A] {
    def pass (v: A) {
      require (va == null, "Value 'a' was already set.")
      va = v
      release()
    }
    def fail (t: Throwable): Unit =
      TripleLatch.this.fail (t)
  }

  val cbB = new Callback [B] {
    def pass (v: B) {
      require (vb == null, "Value 'b' was already set.")
      vb = v
      release()
    }
    def fail (t: Throwable): Unit =
      TripleLatch.this.fail (t)
  }

  val cbC = new Callback [C] {
    def pass (v: C) {
      require (vc == null, "Value 'c' was already set.")
      vc = v
      release()
    }
    def fail (t: Throwable): Unit =
      TripleLatch.this.fail (t)
  }}
