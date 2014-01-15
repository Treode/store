package com.treode.async

private class CountingLatch (count: Int, cb: Callback [Unit])
extends AbstractLatch [Unit] (count, cb) with Callback [Any] {

  private var thrown = List.empty [Throwable]

  init()

  def value = ()

  def pass (v: Any): Unit = synchronized {
    release()
  }}
