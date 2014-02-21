package com.treode.async

private class IndexedLatch [A] (count: Int, cb: Callback [Seq [A]]) (implicit manifest: Manifest [A])
extends AbstractLatch [Seq [A]] (count, cb) with Callback [(Int, A)] {

  private var values = new Array [A] (count)

  init()

  def value = values.toSeq

  def pass (x: (Int, A)): Unit = synchronized {
    val (i, v) = x
    values (i) = v
    release()
  }}
