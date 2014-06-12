package com.treode.store.tier

import scala.util.{Failure, Success}

import com.treode.async.{Async, Callback}
import com.treode.async.implicits._
import com.treode.disk.{Disk, Position}
import com.treode.store.{Bytes, Bound, Cell, Key, Residents, StorePicklers, TxClock}

import Async.async

private case class Tier (
    gen: Long,
    root: Position,
    bloom: Position,
    residents: Residents,
    keys: Long,
    entries: Long,
    earliest: TxClock,
    latest: TxClock,
    diskBytes: Long
) {

  def get (desc: TierDescriptor, key: Bytes, time: TxClock) (implicit disk: Disk): Async [Option [Cell]] =
    async { cb =>

      import desc.pager

      val target = Bound.Inclusive (Key (key, time))

      val loop = Callback.fix [TierPage] { loop => {

        case Success (p: IndexPage) =>
          val i = p.ceiling (target)
          if (i == p.size) {
            cb.pass (None)
          } else {
            val e = p.get (i)
            pager.read (e.pos) .run (loop)
          }

        case Success (p: CellPage) =>
          val i = p.ceiling (target)
          if (i == p.size)
            cb.pass (None)
          else
            cb.pass (Some (p.get (i)))

        case Success (p @ _) =>
          cb.fail (new MatchError (p))

        case Failure (t) =>
          cb.fail (t)
      }}

      pager.read (bloom) .run {

        case Success (bloom: BloomFilter) if bloom.contains (key) =>
          pager.read (root) .run (loop)

        case Success (_: BloomFilter) =>
          cb.pass (None)

        case Success (p @ _) =>
          cb.fail (new MatchError (p))

        case Failure (t) =>
          cb.fail (t)
      }}

  def estimate (other: Residents): Long =
    (keys.toDouble * residents.stability (other) * 1.1).toLong

  override def toString: String =
    s"Tier($gen,$root,$bloom)"
}

private object Tier {

  val pickler = {
    import StorePicklers._
    wrap (ulong, pos, pos, residents, ulong, ulong, txClock, txClock, ulong)
    .build ((Tier.apply _).tupled)
    .inspect (v =>
      (v.gen, v.root, v.bloom, v.residents, v.keys, v.entries, v.earliest, v.latest, v.diskBytes))
  }}
