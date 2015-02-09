/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.store.tier

import scala.util.{Failure, Success}

import com.treode.async.{Async, BatchIterator, Callback, Scheduler}
import com.treode.async.implicits._
import com.treode.disk.{Disk, Position}
import com.treode.store.{Bytes, Bound, Cell, CellIterator, Key, TxClock}

import Async.async

private abstract class TierIterator (
    desc: TierDescriptor,
    root: Position
) (implicit
    disk: Disk
) extends CellIterator {

  class Batch (f: Iterable [Cell] => Async [Unit], cb: Callback [Unit]) {

    import desc.pager

    // A stack of the index pages descended into, and the current entry for each page.
    private var stack = List.empty [(IndexPage, Int)]

    val _push = cb.continue { p: TierPage =>
      push (p)
    }

    val _next = cb.continue { _: Unit =>
      next()
    }

    // Descend to the first leaf page by following the first entry of each index page.
    def start() {

      val loop = Callback.fix [TierPage] { loop => {

        case Success (p: IndexPage) =>
          // Push this index page onto the stack, and descend into the next page.
          val e = p.get (0)
          stack ::= (p, 0)
          pager.read (e.pos) .run (loop)

        case Success (p: CellPage) =>
          cb.callback (body (p, 0))

        case Success (p) =>
          cb.fail (new MatchError (p))

        case Failure (t) =>
          cb.fail (t)
      }}

      pager.read (root) .run (loop)
    }

    // Descend to the leaf which holds the start key by looking up the key at each index page.
    def start (start: Bound [Key]) {

      val loop = Callback.fix [TierPage] { loop => {

        case Success (p: IndexPage) =>
          val i = p.ceiling (start)
          if (i == p.size) {
            // The start key is beyond the last entry of this tier.
            cb.pass (None)
          } else {
            // Push this index page onto the stack, and descend into the next page.
            val e = p.get (i)
            stack ::= (p, i)
            pager.read (e.pos) .run (loop)
          }

        case Success (p: CellPage) =>
          val i = p.ceiling (start)
          if (i == p.size)
            cb.pass (None)
          else
            cb.callback (body (p, i))

        case Success (p) =>
          cb.fail (new MatchError (p))

        case Failure (t) =>
          cb.fail (t)
      }}

      pager.read (root) .run (loop)
    }

    def push (p: TierPage): Option [Unit] = {
      p match {
        case p: IndexPage =>
          val e = p.get (0)
          stack ::= (p, 0)
          pager.read (e.pos) .run (_push)
          None
        case p: CellPage =>
          body (p, 0)
      }}

    def body (page: CellPage, index: Int): Option [Unit] = {
      f (page.entries.drop (index)) run (_next)
      None
    }

    def next(): Option [Unit] = {
      if (!stack.isEmpty) {
        // Pop the recent index page off the stack.
        var b = stack.head._1
        var i = stack.head._2 + 1
        stack = stack.tail
        // Continue popping spent index pages off the stack.
        while (i == b.size && !stack.isEmpty) {
          b = stack.head._1
          i = stack.head._2 + 1
          stack = stack.tail
        }
        // Advance to the next entry in the unspent index page, push it onto the stack and descend
        // into the next page.
        if (i < b.size) {
          stack ::= (b, i)
          pager.read (b.get (i) .pos) .run (_push)
          None
        } else {
          Some (())
        }
      } else {
        Some (())
      }}}}

private object TierIterator {

  class FromBeginning (
      desc: TierDescriptor,
      root: Position
  ) (implicit
      disk: Disk
  ) extends TierIterator (desc, root) {

    def batch (f: Iterable [Cell] => Async [Unit]): Async [Unit] =
      async (new Batch (f, _) .start())
  }

  class FromKey (
      desc: TierDescriptor,
      root: Position,
      start: Bound [Key]
  ) (implicit
      disk: Disk
  ) extends TierIterator (desc, root) {

    def batch (f: Iterable [Cell] => Async [Unit]): Async [Unit] =
      async (new Batch (f, _) .start (start))
  }

  def apply (
      desc: TierDescriptor,
      root: Position
  ) (implicit
      disk: Disk,
      scheduler: Scheduler
  ): CellIterator =
    (new FromBeginning (desc, root))

  def apply (
      desc: TierDescriptor,
      root: Position,
      start: Bound [Key]
  ) (implicit
      disk: Disk,
      scheduler: Scheduler
  ): CellIterator =
    (new FromKey (desc, root, start))

  def adapt (tier: MemTier) (implicit scheduler: Scheduler): CellIterator =
    tier.entrySet.batch.map (memTierEntryToCell _)

  def adapt (tier: MemTier, start: Bound [Key]) (implicit scheduler: Scheduler): CellIterator =
    adapt (tier.tailMap (start.bound, start.inclusive))

  def merge (
      desc: TierDescriptor,
      tiers: Tiers
  ) (implicit
      scheduler: Scheduler,
      disk: Disk
  ): CellIterator = {

    val allTiers = new Array [CellIterator] (tiers.size)
    for (i <- 0 until tiers.size)
      allTiers (i) = TierIterator (desc, tiers (i) .root)

    BatchIterator.merge (allTiers)
  }

  def merge (
      desc: TierDescriptor,
      primary: MemTier,
      secondary: MemTier,
      tiers: Tiers
  ) (implicit
      scheduler: Scheduler,
      disk: Disk
  ): CellIterator = {

    val allTiers = new Array [CellIterator] (tiers.size + 2)
    allTiers (0) = adapt (primary)
    allTiers (1) = adapt (secondary)
    for (i <- 0 until tiers.size)
      allTiers (i + 2) = TierIterator (desc, tiers (i) .root)

    BatchIterator.merge (allTiers)
  }

  def merge (
      desc: TierDescriptor,
      start: Bound [Key],
      primary: MemTier,
      secondary: MemTier,
      tiers: Tiers
  ) (implicit
      scheduler: Scheduler,
      disk: Disk
  ): CellIterator = {

    val allTiers = new Array [CellIterator] (tiers.size + 2)
    allTiers (0) = adapt (primary, start)
    allTiers (1) = adapt (secondary, start)
    for (i <- 0 until tiers.size; tier = tiers (i))
      allTiers (i + 2) = TierIterator (desc, tier.root, start)

    BatchIterator.merge (allTiers)
  }}
