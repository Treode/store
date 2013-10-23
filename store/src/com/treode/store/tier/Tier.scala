package com.treode.store.tier

import com.treode.cluster.concurrent.Callback
import com.treode.store.{Bytes, TxClock}
import com.treode.store.disk.{Block, DiskSystem}

object Tier {

  def read (disk: DiskSystem, root: Long, key: Bytes, time: TxClock, cb: Callback [Option [Cell]]) {

    val loop = new Callback [Block] {

      def apply (b: Block) {
        b match {
          case b: IndexBlock =>
            val i = b.find (key, time)
            if (i == b.size) {
              cb (None)
            } else {
              val e = b.get (i)
              disk.read (e.pos, this)
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

    disk.read (root, loop)
  }}
