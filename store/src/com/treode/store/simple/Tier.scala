package com.treode.store.simple

import com.treode.cluster.concurrent.Callback
import com.treode.store.{Bytes, TxClock}
import com.treode.store.disk.{Block, Disk}

object Tier {

  def read (disk: Disk, root: Long, key: Bytes, cb: Callback [Option [Cell]]) {

    val loop = new Callback [Block] {

      def apply (b: Block) {
        b match {
          case b: IndexBlock =>
            val i = b.find (key)
            if (i == b.size) {
              cb (None)
            } else {
              val e = b.get (i)
              disk.read (e.pos, this)
            }
          case b: CellBlock =>
            val i = b.find (key)
            if (i == b.size) {
              cb (None)
            } else {
              val e = b.get (i)
              if (e.key == key)
                cb (Some (e))
              else
                cb (None)
            }}}

      def fail (t: Throwable) = cb.fail (t)
    }

    disk.read (root, loop)
  }}
