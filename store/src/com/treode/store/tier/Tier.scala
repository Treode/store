package com.treode.store.tier

import com.treode.async.{Async, Callback}
import com.treode.disk.{Disks, Position}
import com.treode.store.{Bytes, StorePicklers}

import Async.async

private case class Tier (gen: Long, root: Position) {

  private def ceiling (desc: TierDescriptor [_, _], key: Bytes, cb: Callback [Option [Cell]]) (
      implicit disks: Disks) {

    import desc.pager

    val loop = new Callback [TierPage] {

      def pass (p: TierPage) {
        p match {
          case p: IndexPage =>
            val i = p.ceiling (key)
            if (i == p.size) {
              cb.pass (None)
            } else {
              val e = p.get (i)
              pager.read (e.pos) .run (this)
            }
          case p: CellPage =>
            val i = p.ceiling (key)
            if (i == p.size)
              cb.pass (None)
            else
              cb.pass (Some (p.get (i)))
        }}

      def fail (t: Throwable) = cb.fail (t)
    }

    pager.read (root) .run (loop)
  }

  def ceiling (desc: TierDescriptor [_, _], key: Bytes) (implicit disks: Disks): Async [Option [Cell]] =
    async (ceiling (desc, key, _))
}

private object Tier {

  val pickler = {
    import StorePicklers._
    wrap (ulong, pos)
    .build ((Tier.apply _).tupled)
    .inspect (v => (v.gen, v.root))
  }}
