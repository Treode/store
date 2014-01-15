package com.treode.async

private class ArrayLatch [A] (count: Int, cb: Callback [Array [A]]) (implicit manifest: Manifest [A])
extends AbstractLatch [Array [A]] (count, cb) with Callback [(Int, A)] {

  private var values = new Array [A] (count)

  init()

  def value = values

  def pass (x: (Int, A)): Unit = synchronized {
    val (i, v) = x
    values (i) = v
    release()
  }}
