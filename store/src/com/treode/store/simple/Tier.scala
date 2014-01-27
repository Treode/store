package com.treode.store.simple

import com.treode.async.Callback
import com.treode.disk.{Disks, Position}
import com.treode.store.{Bytes, TxClock}

private object Tier {

  def read (root: Position, key: Bytes, cb: Callback [Option [SimpleCell]]) (
      implicit disks: Disks) {

    val loop = new Callback [TierPage] {

      def pass (p: TierPage) {
        p match {
          case p: IndexPage =>
            val i = p.find (key)
            if (i == p.size) {
              cb (None)
            } else {
              val e = p.get (i)
              TierPage.page.read (e.pos, this)
            }
          case p: CellPage =>
            val i = p.find (key)
            if (i == p.size) {
              cb (None)
            } else {
              val e = p.get (i)
              if (e.key == key)
                cb (Some (e))
              else
                cb (None)
            }}}

      def fail (t: Throwable) = cb.fail (t)
    }

    TierPage.page.read (root, loop)
  }}
