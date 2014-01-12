package com.treode.store.disk2

import com.treode.buffer.PagedBuffer
import com.treode.pickle.{Pickler, unpickle}

private trait PickledReplay {

  def prepare (time: Long, buf: PagedBuffer): Replay
}

private object PickledReplay {

  def apply [A] (p: Pickler [A], id: TypeId, f: A => Any): PickledReplay =
    new PickledReplay {
      def prepare (time: Long, buf: PagedBuffer): Replay =
        Replay (time, id, f, unpickle (p, buf))
  }}
