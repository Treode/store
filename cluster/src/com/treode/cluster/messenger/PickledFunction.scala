package com.treode.cluster.messenger

import com.treode.buffer.PagedBuffer
import com.treode.cluster.Peer
import com.treode.pickle.{Pickler, unpickle}

private trait PickledFunction {

  def apply (from: Peer, buffer: PagedBuffer)
}

private object PickledFunction {

  def apply [A] (p: Pickler [A], f: (A, Peer) => Any): PickledFunction =
    new PickledFunction {
      def apply (from: Peer, buf: PagedBuffer) = {
        val msg = unpickle (p, buf)
        f (msg, from)
      }}}
