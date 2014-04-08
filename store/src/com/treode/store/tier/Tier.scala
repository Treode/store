package com.treode.store.tier

import scala.util.{Failure, Success}

import com.treode.async.{Async, AsyncImplicits, Callback}
import com.treode.disk.{Disks, Position}
import com.treode.store.{Bytes, Cell, StorePicklers, TxClock}

import Async.async
import AsyncImplicits._

private case class Tier (
    gen: Long,
    root: Position,
    entries: Long,
    earliest: TxClock,
    latest: TxClock,
    entryBytes: Long,
    diskBytes: Long
) {

  private def ceiling (
      desc: TierDescriptor [_, _],
      key: Bytes,
      time: TxClock,
      cb: Callback [Option [Cell]]
  ) (
      implicit disks: Disks
  ) {

    import desc.pager

    val loop = Callback.fix [TierPage] { loop => {

      case Success (p: IndexPage) =>
        val i = p.ceiling (key, time)
        if (i == p.size) {
          cb.pass (None)
        } else {
          val e = p.get (i)
          pager.read (e.pos) .run (loop)
        }

      case Success (p: CellPage) =>
        val i = p.ceiling (key, time)
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

  def ceiling (desc: TierDescriptor [_, _], key: Bytes, time: TxClock) (implicit disks: Disks): Async [Option [Cell]] =
    async (ceiling (desc, key, time, _))

  override def toString: String =
    s"Tier($gen, $root)"
}

private object Tier {

  val pickler = {
    import StorePicklers._
    wrap (ulong, pos, ulong, txClock, txClock, ulong, ulong)
    .build ((Tier.apply _).tupled)
    .inspect (v => (v.gen, v.root, v.entries, v.earliest, v.latest, v.entryBytes, v.diskBytes))
  }}
