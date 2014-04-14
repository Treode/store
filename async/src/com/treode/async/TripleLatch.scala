package com.treode.async

import scala.util.{Failure, Success}

private class TripleLatch [A, B, C] (cb: Callback [(A, B, C)])
extends AbstractLatch [(A, B, C)] (3, cb) {

  private var va: A = null.asInstanceOf [A]
  private var vb: B = null.asInstanceOf [B]
  private var vc: C = null.asInstanceOf [C]

  def value: (A, B, C) = (va, vb, vc)

  init()

  val cbA: Callback [A] = {
    case Success (v) => synchronized {
      require (va == null, "Value 'a' was already set.")
      va = v
      release()
    }
    case Failure (t) => synchronized {
      failure (t)
    }}

  val cbB: Callback [B] = {
    case Success (v) => synchronized {
      require (vb == null, "Value 'b' was already set.")
      vb = v
      release()
    }
    case Failure (t) => synchronized {
      failure (t)
    }}

  val cbC: Callback [C] = {
    case Success (v) => synchronized {
      require (vc == null, "Value 'c' was already set.")
      vc = v
      release()
    }
    case Failure (t) => synchronized {
      failure (t)
    }}}
