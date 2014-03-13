package com.treode.async

import scala.util.{Failure, Success, Try}

private class MapLatch [K, V] (count: Int, cb: Callback [Map [K, V]])
extends AbstractLatch [Map [K, V]] (count, cb) with Callback [(K, V)] {

  private var map = Map.newBuilder [K, V]
  map.sizeHint (count)

  init()

  def value = map.result

  def apply (v: Try [(K, V)]): Unit = synchronized {
    v match {
      case Success ((k, v)) =>
        map += (k -> v)
        release()
      case Failure (t) => failure (t)
    }}}
