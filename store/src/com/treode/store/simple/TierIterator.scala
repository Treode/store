package com.treode.store.simple

import com.treode.async.{AsyncIterator, Callback, callback, delay}
import com.treode.disk.{Disks, Position}

private class TierIterator (pager: TierPage.Descriptor) (implicit disks: Disks)
extends AsyncIterator [SimpleCell] {

  private var stack = List.empty [(IndexPage, Int)]
  private var page: CellPage = null
  private var index = 0

  private def find (pos: Position, cb: Callback [TierIterator]) {

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

  def next (cb: Callback [SimpleCell]) {
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

  def apply (pager: TierPage.Descriptor, pos: Position, cb: Callback [SimpleIterator]) (
      implicit disks: Disks): Unit =
    new TierIterator (pager) .find (pos, cb)

  def merge (pager: TierPage.Descriptor, primary: MemTier, secondary: MemTier, tiers: Tiers,
      cb: Callback [SimpleIterator]) (implicit disks: Disks) {

    val allBuilt = delay (cb) { iters: Array [SimpleIterator] =>
      AsyncIterator.merge (iters.iterator, cb)
    }

    val oneBuilt = Callback.collate (tiers.size + 2, allBuilt)

    oneBuilt (0, AsyncIterator.adapt (primary))
    oneBuilt (1, AsyncIterator.adapt (secondary))
    for (i <- 0 until tiers.size)
      TierIterator (pager, tiers.pos (i), delay (oneBuilt) (oneBuilt (i+2, _)))
  }}
