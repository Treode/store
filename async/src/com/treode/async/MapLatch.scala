package com.treode.async

private class MapLatch [K, V] (count: Int, cb: Callback [Map [K, V]])
extends AbstractLatch [Map [K, V]] (count, cb) with Callback [(K, V)] {

  private var map = Map.newBuilder [K, V]
  map.sizeHint (count)

  init()

  def value = map.result

  def pass (x: (K, V)): Unit = synchronized {
    val (k, v) = x
    map += (k -> v)
    release()
  }}
