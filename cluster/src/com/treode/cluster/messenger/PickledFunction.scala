package com.treode.cluster.messenger

import com.treode.cluster.Peer
import com.treode.pickle.{Pickler, unpickle}
import io.netty.buffer.ByteBuf

private trait PickledFunction {

  def apply (buffer: ByteBuf, from: Peer)
}

private object PickledFunction {

  def apply [A] (p: Pickler [A], f: (A, Peer) => Any): PickledFunction =
    new PickledFunction {
      def apply (buffer: ByteBuf, from: Peer) = {
        val message = unpickle (p, buffer)
        require (buffer.readableBytes() == 0, "Bytes remain after unpickling message")
        f (message, from)
      }}}
