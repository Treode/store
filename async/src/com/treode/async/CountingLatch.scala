package com.treode.async

import scala.util.{Failure, Success, Try}

private class CountingLatch [A] (count: Int, cb: Callback [Unit])
extends AbstractLatch (count, cb) with Callback [A] {

  private var thrown = List.empty [Throwable]

  init()

  def value = ()

  def apply (v: Try [A]): Unit = synchronized {
    v match {
      case Success (v) => release()
      case Failure (t) => failure (t)
    }}}
