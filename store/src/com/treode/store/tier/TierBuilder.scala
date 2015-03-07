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

import java.util.{ArrayDeque, ArrayList}
import scala.collection.JavaConversions._

import com.treode.async.{Async, Scheduler}
import com.treode.disk.{Disk, Position}
import com.treode.store.{Bytes, Cell, CellIterator, Residents, StoreConfig, TableId, TxClock}

import Async.{async, guard, supply, when}

private class TierBuilder (
    desc: TierDescriptor,
    id: TableId,
    gen: Long,
    residents: Residents,
    bloom: BloomFilter
) (implicit
    scheduler: Scheduler,
    disk: Disk,
    config: StoreConfig
) {

  import desc.pager
  import scheduler.whilst

  private class IndexNode (val height: Int) {

    val entries = new ArrayList [IndexEntry] (1024)

    def size = entries.size

    var byteSize = 0

    def add (entry: IndexEntry, byteSize: Int) {
      entries.add (entry)
      this.byteSize += byteSize
    }}

  private class CellsNode {

    val entries = new ArrayList [Cell] (256)

    def size = entries.size

    def isEmpty = entries.isEmpty

    def last = entries.last

    var byteSize = 0

    def add (entry: Cell, byteSize: Int) {
      entries.add (entry)
      this.byteSize += byteSize
    }}

  private val stack = new ArrayDeque [IndexNode]
  private val rstack = new ArrayDeque [IndexNode]
  private var cells = new CellsNode
  private var earliestTime = TxClock.MaxValue
  private var latestTime = TxClock.MinValue
  private var totalKeys = 0L
  private var totalEntries = 0L
  private var totalDiskBytes = 0L

  private def push (key: Bytes, time: TxClock, pos: Position, height: Int) {
    val node = new IndexNode (height)
    val entry = IndexEntry (key, time, pos)
    node.add (entry, entry.byteSize)
    stack.push (node)
  }

  private def rpush (key: Bytes, time: TxClock, pos: Position, height: Int) {
    val node = new IndexNode (height)
    val entry = IndexEntry (key, time, pos)
    node.add (entry, entry.byteSize)
    rstack.push (node)
  }

  private def rpop() {
    while (!rstack.isEmpty)
      stack.push (rstack.pop())
  }

  private def write (page: TierPage): Async [Position] =
    for {
      pos <- pager.write (id.id, gen, page)
    } yield {
      totalDiskBytes += pos.length
      pos
    }

  private def add (key: Bytes, time: TxClock, pos: Position, height: Int): Async [Unit] =
    guard {

      val node = stack.peek

      if (stack.isEmpty || height < node.height) {
        push (key, time, pos, height)
        rpop()
        supply (())

      } else {

        val entry = IndexEntry (key, time, pos)
        val entryByteSize = entry.byteSize

        // Ensure that an index page has at least two entries.
        if (node.byteSize + entryByteSize < config.targetPageBytes || node.size < 2) {
          node.add (entry, entryByteSize)
          rpop()
          supply (())

        } else {
          stack.pop()
          val page = IndexPage (node.entries)
          val last = page.last
          for {
            pos2 <- write (page)
            _ = rpush (key, time, pos, height)
            _ <- add (last.key, last.time, pos2, height+1)
          } yield ()
        }}}

  def add (cell: Cell): Async [Unit] =
    guard {

      val time = cell.time
      val byteSize = cell.byteSize

      // Require that user adds entries in sorted order; count unique keys.
      if (cells.isEmpty) {
        totalKeys += 1
      } else {
        val last = cells.last
        require (last < cell, "Cells must be added to builder in order.")
        if (last.key != cell.key) totalKeys += 1
      }

      if (earliestTime > time) earliestTime = time
      if (latestTime < time) latestTime = time
      totalEntries += 1

      bloom.put (cell.key)

      // Ensure that a value page has at least one entry.
      if (cells.byteSize + byteSize < config.targetPageBytes || cells.size < 1) {
        cells.add (cell, byteSize)
        supply (())

      } else {
        val page = TierCellPage (cells.entries)
        cells = new CellsNode
        cells.add (cell, byteSize)
        val last = page.last
        for {
          pos <- write (page)
          _ <- add (last.key, last.time, pos, 0)
        } yield ()
      }}

  private def pop (page: CellPage, pos0: Position): Async [Position] = {
    val node = stack.pop()
    for {
      pos1 <-
        if (node.size > 1)
          write (IndexPage (node.entries))
        else
          supply (pos0)
      _ <- when (!stack.isEmpty) (add (page.last.key, page.last.time, pos1, node.height+1))
    } yield pos1
  }

  def result(): Async [Tier] = {
    val page = TierCellPage (cells.entries)
    var pagePos: Position = null
    for {
      _ <- write (page) .map (pagePos = _)
      _ <- when (cells.size > 0) (add (page.last.key, page.last.time, pagePos, 0))
      _ <- whilst (!stack.isEmpty) (pop (page, pagePos) .map (pagePos = _))
      bloomPos <- write (bloom)
    } yield {
      Tier (
          gen, pagePos, bloomPos, residents, totalKeys, totalEntries, earliestTime, latestTime,
          totalDiskBytes)
    }}}

private object TierBuilder {

  def build (
      desc: TierDescriptor,
      id: TableId,
      gen: Long,
      est: Long,
      residents: Residents,
      iter: CellIterator
  ) (implicit
      scheduler: Scheduler,
      disk: Disk,
      config: StoreConfig
  ): Async [Tier] = {
    val bloom = BloomFilter (math.max (1L, est), config.falsePositiveProbability)
    val builder = new TierBuilder (desc, id, gen, residents, bloom)
    for {
      _ <- iter.flatten.foreach (builder.add (_))
      tier <- builder.result()
    } yield tier
  }}
