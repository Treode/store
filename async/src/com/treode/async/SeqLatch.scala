package com.treode.async

import scala.util.{Failure, Success, Try}

private class SeqLatch [A] (count: Int, cb: Callback [Seq [A]]) (implicit manifest: Manifest [A])
extends AbstractLatch [Seq [A]] (count, cb) with Callback [A] {

  private var values = Seq.newBuilder [A]
  values.sizeHint (count)

  init()

  def value = values.result

  def apply (v: Try [A]): Unit = synchronized {
    v match {
      case Success (v) =>
        values += v
        release()
      case Failure (t) => failure (t)
    }}}
