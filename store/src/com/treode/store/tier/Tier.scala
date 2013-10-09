package com.treode.store.tier

import com.treode.cluster.concurrent.Callback
import com.treode.store.{Bytes, Cell, TxClock}

class Tier (cache: BlockCache, root: Long) {

  private def find (pos: Long, key: Bytes, time: TxClock, cb: Callback [Option [Cell]]) {

    val loop = new Callback [Block] {

      def apply (b: Block) {
        b match {
          case b: IndexBlock =>
            val i = b.find (key, time)
            if (i == b.size) {
              cb (None)
            } else {
              val e = b.get (i)
              cache.get (e.pos, this)
            }
          case b: CellBlock =>
            val i = b.find (key, time)
            if (i == b.size) {
              cb (None)
            } else {
              val e = b.get (i)
              if (e.key == key && e.time <= time)
                cb (Some (e))
              else
                cb (None)
            }}}

      def fail (t: Throwable) = cb.fail (t)
    }

    cache.get (pos, loop)
  }

  def read (key: Bytes, time: TxClock, cb: Callback [Option [Cell]]): Unit =
    find (root, key, time, cb)
}
