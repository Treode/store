package com.treode.store.tier

import scala.util.{Failure, Success}

import com.treode.async.{Async, AsyncImplicits, Callback}
import com.treode.disk.{Disks, Position}
import com.treode.store.{Bytes, StorePicklers}

import Async.async
import AsyncImplicits._

private case class Tier (gen: Long, root: Position) {

  private def ceiling (
      desc: TierDescriptor [_, _],
      key: Bytes,
      cb: Callback [Option [TierCell]]
  ) (
      implicit disks: Disks
  ) {

    import desc.pager

    val loop = Callback.fix [TierPage] { loop => {

      case Success (p: IndexPage) =>
        val i = p.ceiling (key)
        if (i == p.size) {
          cb.pass (None)
        } else {
          val e = p.get (i)
          pager.read (e.pos) .run (loop)
        }

      case Success (p: TierCellPage) =>
        val i = p.ceiling (key)
        if (i == p.size)
          cb.pass (None)
        else
          cb.pass (Some (p.get (i)))

      case Success (_) =>
        cb.fail (new MatchError)

      case Failure (t) =>
        cb.fail (t)
    }}

    pager.read (root) .run (loop)
  }

  def ceiling (desc: TierDescriptor [_, _], key: Bytes) (implicit disks: Disks): Async [Option [TierCell]] =
    async (ceiling (desc, key, _))
}

private object Tier {

  val pickler = {
    import StorePicklers._
    wrap (ulong, pos)
    .build ((Tier.apply _).tupled)
    .inspect (v => (v.gen, v.root))
  }}
