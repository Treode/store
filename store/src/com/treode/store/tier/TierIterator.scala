package com.treode.store.tier

import scala.collection.JavaConversions._
import scala.util.{Failure, Success}

import com.treode.async.{Async, AsyncImplicits, AsyncIterator, Callback, Scheduler}
import com.treode.disk.{Disks, Position}
import com.treode.store.{Bytes, Cell, CellIterator, TxClock}

import Async.async
import AsyncImplicits._

private abstract class TierIterator (
    desc: TierDescriptor [_, _],
    root: Position
) (implicit
    disks: Disks
) extends CellIterator {

  class Foreach (f: (Cell, Callback [Unit]) => Any, cb: Callback [Unit]) {

    import desc.pager

    private var stack = List.empty [(IndexPage, Int)]
    private var page: CellPage = null
    private var index = 0

    val _push = cb.continue { p: TierPage =>
      push (p)
    }

    val _next = cb.continue { _: Unit =>
      next()
    }

    def start() {

      val loop = Callback.fix [TierPage] { loop => {

        case Success (p: IndexPage) =>
          val e = p.get (0)
          stack ::= (p, 0)
          pager.read (e.pos) .run (loop)

        case Success (p: CellPage) =>
          page = p
          index = 0
          next()

        case Success (p) =>
          cb.fail (new MatchError (p))

        case Failure (t) =>
          cb.fail (t)
      }}

      pager.read (root) .run (loop)
    }

    def start (key: Bytes, time: TxClock) {

      val loop = Callback.fix [TierPage] { loop => {

        case Success (p: IndexPage) =>
          val i = p.ceiling (key, time)
          if (i == p.size) {
            cb.pass (None)
          } else {
            val e = p.get (i)
            stack ::= (p, i)
            pager.read (e.pos) .run (loop)
          }

        case Success (p: CellPage) =>
          val i = p.ceiling (key, time)
          if (i == p.size) {
            cb.pass (None)
          } else {
            page = p
            index = i
            next()
          }

        case Success (p) =>
          cb.fail (new MatchError (p))

        case Failure (t) =>
          cb.fail (t)
      }}

      pager.read (root) .run (loop)
    }

    def push (p: TierPage) {
      p match {
        case p: IndexPage =>
          val e = p.get (0)
          stack ::= (p, 0)
          pager.read (e.pos) .run (_push)
        case p: CellPage =>
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
      }}}}

private object TierIterator {

  class FromBeginning (
      desc: TierDescriptor [_, _],
      root: Position
  ) (implicit
      disks: Disks
  ) extends TierIterator (desc, root) {

    def _foreach (f: (Cell, Callback [Unit]) => Any): Async [Unit] =
      async (new Foreach (f, _) .start())
  }

  class FromKey (
      desc: TierDescriptor [_, _],
      root: Position,
      key: Bytes,
      time: TxClock
  ) (implicit
      disks: Disks
  ) extends TierIterator (desc, root) {

    def _foreach (f: (Cell, Callback [Unit]) => Any): Async [Unit] =
      async (new Foreach (f, _) .start (key, time))
  }

  def apply (
      desc: TierDescriptor [_, _],
      root: Position
  ) (implicit
      disks: Disks
  ): CellIterator =
    new FromBeginning (desc, root)

  def apply (
      desc: TierDescriptor [_, _],
      root: Position,
      key: Bytes,
      time: TxClock
  ) (implicit
      disks: Disks
  ): CellIterator =
    new FromKey (desc, root, key, time)

  def adapt (tier: MemTier) (implicit scheduler: Scheduler): CellIterator =
    tier.entrySet.iterator.map (memTierEntryToCell _) .async

  def adapt (tier: MemTier, key: Bytes, time: TxClock) (implicit scheduler: Scheduler): CellIterator =
    adapt (tier.tailMap (MemKey (key, time), true))

  def merge (
      desc: TierDescriptor [_, _],
      primary: MemTier,
      secondary: MemTier,
      tiers: Tiers
  ) (implicit
      scheduler: Scheduler,
      disks: Disks
  ): CellIterator = {

    val allTiers = new Array [CellIterator] (tiers.size + 2)
    allTiers (0) = adapt (primary)
    allTiers (1) = adapt (secondary)
    for (i <- 0 until tiers.size)
      allTiers (i + 2) = TierIterator (desc, tiers (i) .root)

    AsyncIterator.merge (allTiers)
  }

  def merge (
      desc: TierDescriptor [_, _],
      key: Bytes,
      time: TxClock,
      primary: MemTier,
      secondary: MemTier,
      tiers: Tiers
  ) (implicit
      scheduler: Scheduler,
      disks: Disks
  ): CellIterator = {

    val allTiers = new Array [CellIterator] (tiers.size + 2)
    allTiers (0) = adapt (primary, key, time)
    allTiers (1) = adapt (secondary, key, time)
    for (i <- 0 until tiers.size; tier = tiers (i))
      allTiers (i + 2) = TierIterator (desc, tier.root, key, time)

    AsyncIterator.merge (allTiers)
  }}
