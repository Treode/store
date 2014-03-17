package com.treode.store.tier

import scala.collection.JavaConversions._
import com.treode.async.{Async, AsyncConversions, AsyncIterator, Callback, Scheduler}
import com.treode.disk.{Disks, Position}

import Async.async
import AsyncConversions._

private class TierIterator (desc: TierDescriptor [_, _], root: Position) (
    implicit disks: Disks) extends TierCellIterator {

  class Foreach (f: (TierCell, Callback [Unit]) => Any, cb: Callback [Unit]) {

    import desc.pager

    private var stack = List.empty [(IndexPage, Int)]
    private var page: TierCellPage = null
    private var index = 0

    val _push = cb.continue { p: TierPage =>
      push (p)
    }

    val _next = cb.continue { _: Unit =>
      next()
    }

    def start() {
      pager.read (root) .run (_push)
    }

    def push (p: TierPage) {
      p match {
        case p: IndexPage =>
          val e = p.get (0)
          stack ::= (p, 0)
          pager.read (e.pos) .run (_push)
        case p: TierCellPage =>
          page = p
          index = 0
          next()
      }}

    def next() {
      if (index < page.size) {
        val entry = page.get (index)
        index += 1
        f (entry, _next)
      } else if (!stack.isEmpty) {
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
          pager.read (b.get (i) .pos) .run (_push)
        } else {
          cb.pass()
        }
      } else {
        cb.pass()
      }}}

  def _foreach (f: (TierCell, Callback [Unit]) => Any): Async [Unit] =
    async (new Foreach (f, _) .start())
}

private object TierIterator {

  def apply (desc: TierDescriptor [_, _], root: Position) (implicit disks: Disks): TierCellIterator =
    new TierIterator (desc, root)

  def adapt (tier: MemTier) (implicit scheduler: Scheduler): TierCellIterator =
     tier.entrySet.iterator.map (TierCell.apply _) .async

  def merge (desc: TierDescriptor [_, _], primary: MemTier, secondary: MemTier, tiers: Tiers) (
      implicit scheduler: Scheduler, disks: Disks): TierCellIterator = {

    val allTiers = new Array [TierCellIterator] (tiers.size + 2)
    allTiers (0) = adapt (primary)
    allTiers (1) = adapt (secondary)
    for (i <- 0 until tiers.size)
      allTiers (i + 2) = TierIterator (desc, tiers (i) .root)

    AsyncIterator.merge (allTiers)
  }}
