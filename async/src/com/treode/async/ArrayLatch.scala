package com.treode.async

import scala.util.{Failure, Success, Try}

private class ArrayLatch [A] (count: Int, cb: Callback [Seq [A]]) (implicit manifest: Manifest [A])
extends AbstractLatch [Seq [A]] (count, cb) with Callback [(Int, A)] {

  private var values = new Array [A] (count)

  init()

  def value = values.toSeq

  def apply (v: Try [(Int, A)]): Unit = synchronized {
    v match {
      case Success ((i, v)) =>
        values (i) = v
        release()
      case Failure (t) => failure (t)
    }}}
