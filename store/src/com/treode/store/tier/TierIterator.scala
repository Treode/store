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

import scala.collection.JavaConversions._
import scala.util.{Failure, Success}

import com.treode.async.{Async, AsyncIterator, Callback, Scheduler}
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

  class Foreach (f: Cell => Async [Unit], cb: Callback [Unit]) {

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
          cb.callback (next())

        case Success (p) =>
          cb.fail (new MatchError (p))

        case Failure (t) =>
          cb.fail (t)
      }}

      pager.read (root) .run (loop)
    }

    def start (start: Bound [Key]) {

      val loop = Callback.fix [TierPage] { loop => {

        case Success (p: IndexPage) =>
          val i = p.ceiling (start)
          if (i == p.size) {
            cb.pass (None)
          } else {
            val e = p.get (i)
            stack ::= (p, i)
            pager.read (e.pos) .run (loop)
          }

        case Success (p: CellPage) =>
          val i = p.ceiling (start)
          if (i == p.size) {
            cb.pass (None)
          } else {
            page = p
            index = i
            cb.callback (next())
          }

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
          page = p
          index = 0
          next()
      }}

    def next(): Option [Unit] = {
      if (index < page.size) {
        val entry = page.get (index)
        index += 1
        f (entry) run (_next)
        None
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

    def foreach (f: Cell => Async [Unit]): Async [Unit] =
      async (new Foreach (f, _) .start())
  }

  class FromKey (
      desc: TierDescriptor,
      root: Position,
      start: Bound [Key]
  ) (implicit
      disk: Disk
  ) extends TierIterator (desc, root) {

    def foreach (f: Cell => Async [Unit]): Async [Unit] =
      async (new Foreach (f, _) .start (start))
  }

  def apply (
      desc: TierDescriptor,
      root: Position
  ) (implicit
      disk: Disk
  ): CellIterator =
    new FromBeginning (desc, root)

  def apply (
      desc: TierDescriptor,
      root: Position,
      start: Bound [Key]
  ) (implicit
      disk: Disk
  ): CellIterator =
    new FromKey (desc, root, start)

  def adapt (tier: MemTier) (implicit scheduler: Scheduler): CellIterator =
    tier.entrySet.iterator.map (memTierEntryToCell _) .async

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

    AsyncIterator.merge (allTiers)
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

    AsyncIterator.merge (allTiers)
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

    AsyncIterator.merge (allTiers)
  }}
