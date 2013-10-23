package com.treode.store.tier

import com.treode.cluster.concurrent.Callback
import com.treode.store.{Bytes, TxClock}
import com.treode.store.disk.{DiskSystem, Page}

object Tier {

  def read (disk: DiskSystem, root: Long, key: Bytes, time: TxClock, cb: Callback [Option [Cell]]) {

    val loop = new Callback [Page] {

      def pass (p: Page) {
        p match {
          case p: IndexPage =>
            val i = p.find (key, time)
            if (i == p.size) {
              cb (None)
            } else {
              val e = p.get (i)
              disk.read (e.pos, this)
            }
          case p: CellPage =>
            val i = p.find (key, time)
            if (i == p.size) {
              cb (None)
            } else {
              val e = p.get (i)
              if (e.key == key && e.time <= time)
                cb (Some (e))
              else
                cb (None)
            }}}

      def fail (t: Throwable) = cb.fail (t)
    }

    disk.read (root, loop)
  }}
