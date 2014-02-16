package com.treode.store.tier

import scala.collection.JavaConversions._

import com.treode.async.{AsyncIterator, Callback, Latch, Scheduler, callback, continue}
import com.treode.disk.{Disks, Position}

private class TierIterator [K, V] (desc: TierDescriptor [K, V]) (implicit disks: Disks)
extends CellIterator {

  import desc.pager

  private var stack = List.empty [(IndexPage, Int)]
  private var page: CellPage = null
  private var index = 0

  private def find (pos: Position, cb: Callback [TierIterator [K, V]]) {

    val loop = new Callback [TierPage] {

      def pass (p: TierPage) {
        p match {
          case p: IndexPage =>
            val e = p.get (0)
            stack ::= (p, 0)
            pager.read (e.pos, this)
          case p: CellPage =>
            page = p
            index = 0
            cb (TierIterator.this)
        }}

      def fail (t: Throwable) = cb.fail (t)
    }

    pager.read (pos, loop)
  }

  def hasNext: Boolean =
    index < page.size

  def next (cb: Callback [Cell]) {
    val entry = page.get (index)
    index += 1
    if (index == page.size && !stack.isEmpty) {
      var b = stack.head._1
      var i = stack.head._2 + 1
      stack = stack.tail
      while (i == b.size && !stack.isEmpty) {
        b = stack.head._1
        i = stack.head._2 + 1
        stack = stack.tail
      }
      if (i < b.size) {
        stack ::= (b, i)
        find (b.get (i) .pos, callback (_ => cb (entry)))
      } else {
        cb (entry)
      }
    } else {
      cb (entry)
    }}}

private object TierIterator {

  def apply [K, V] (desc: TierDescriptor [K, V], pos: Position, cb: Callback [CellIterator]) (
      implicit disks: Disks): Unit =
    new TierIterator (desc) .find (pos, cb)

  def adapt (tier: MemTier) (implicit scheduler: Scheduler): CellIterator =
    AsyncIterator.adapt (tier.entrySet.iterator.map (Cell.apply _))

  def merge [K, V] (desc: TierDescriptor [K, V], primary: MemTier, secondary: MemTier,
      tiers: Tiers, cb: Callback [CellIterator]) (implicit scheduler: Scheduler, disks: Disks) {

    val allBuilt = continue (cb) { iters: Array [CellIterator] =>
      AsyncIterator.merge (iters.iterator, cb)
    }

    val oneBuilt = Latch.array (tiers.size + 2, allBuilt)

    oneBuilt (0, adapt (primary))
    oneBuilt (1, adapt (secondary))
    for (i <- 0 until tiers.size)
      TierIterator (desc, tiers (i) .root, callback (oneBuilt) ((i+2, _)))
  }}
