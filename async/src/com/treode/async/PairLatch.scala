package com.treode.async

import scala.util.{Failure, Success}

private class PairLatch [A, B] (cb: Callback [(A, B)])
extends AbstractLatch [(A, B)] (2, cb) {

  private var va: A = null.asInstanceOf [A]
  private var vb: B = null.asInstanceOf [B]

  def value: (A, B) = (va, vb)

  init()

  val cbA: Callback [A] = {
    case Success (v) =>
      require (va == null, "Value 'a' was already set.")
      va = v
      release()
    case Failure (t) =>
      failure (t)
  }

  val cbB: Callback [B] = {
    case Success (v) =>
      require (vb == null, "Value 'b' was already set.")
      vb = v
      release()
    case Failure (t) =>
      failure (t)
  }}
