package com.treode.store.simple

import com.treode.async.Callback
import com.treode.disk.{Disks, Position}
import com.treode.store.{Bytes, TxClock}

private object TierReader {

  def read (pager: TierPage.Descriptor, root: Position, key: Bytes,
      cb: Callback [Option [SimpleCell]]) (
      implicit disks: Disks) {

    val epoch = disks.join (cb)

    val loop = new Callback [TierPage] {

      def pass (p: TierPage) {
        p match {
          case p: IndexPage =>
            val i = p.find (key)
            if (i == p.size) {
              epoch (None)
            } else {
              val e = p.get (i)
              pager.read (e.pos, this)
            }
          case p: CellPage =>
            val i = p.find (key)
            if (i == p.size) {
              epoch (None)
            } else {
              val e = p.get (i)
              if (e.key == key)
                epoch (Some (e))
              else
                epoch (None)
            }}}

      def fail (t: Throwable) = epoch.fail (t)
    }

    pager.read (root, loop)
  }}
