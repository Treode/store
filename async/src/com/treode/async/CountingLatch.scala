package com.treode.async

private class CountingLatch [A] (count: Int, cb: Callback [Unit])
extends AbstractLatch [Unit] (count, cb) with Callback [A] {

  private var thrown = List.empty [Throwable]

  init()

  def value = ()

  def pass (v: A): Unit = synchronized {
    release()
  }}
