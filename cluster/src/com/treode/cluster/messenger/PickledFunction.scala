package com.treode.cluster.messenger

import com.esotericsoftware.kryo.io.Input
import com.treode.cluster.Peer
import com.treode.pickle.{Pickler, unpickle}

private trait PickledFunction {

  def apply (from: Peer, input: Input)
}

private object PickledFunction {

  def apply [A] (p: Pickler [A], f: (A, Peer) => Any): PickledFunction =
    new PickledFunction {
      def apply (from: Peer, input: Input) = {
        val message = unpickle (p, input)
        f (message, from)
      }}}
