package com.treode.cluster.messenger

import com.treode.cluster.Peer
import com.treode.pickle.{Buffer, Pickler, unpickle}

private trait PickledFunction {

  def apply (from: Peer, buffer: Buffer)
}

private object PickledFunction {

  def apply [A] (p: Pickler [A], f: (A, Peer) => Any): PickledFunction =
    new PickledFunction {
      def apply (from: Peer, buf: Buffer) = {
        val msg = unpickle (p, buf)
        f (msg, from)
      }}}
