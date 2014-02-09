package com.treode.store.simple

import com.treode.async.Callback
import com.treode.disk.{Disks, Position}
import com.treode.store.{Bytes, StorePicklers}

private case class Tier (gen: Long, root: Position) {

  def read (pager: TierPage.Descriptor, key: Bytes, cb: Callback [Option [SimpleCell]]) (
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
              pager.read (e.pos, this)
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

    pager.read (root, loop)
  }}

private object Tier {

  val pickler = {
    import StorePicklers._
    wrap (ulong, pos)
    .build ((Tier.apply _).tupled)
    .inspect (v => (v.gen, v.root))
  }}
