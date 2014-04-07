package com.treode.store.tier

import java.util.{ArrayDeque, ArrayList}
import scala.collection.JavaConversions._

import com.treode.async.{Async, Scheduler}
import com.treode.disk.{Disks, ObjectId, Position}
import com.treode.store.{Bytes, StoreConfig, TxClock}

import Async.{async, guard, supply, when}

private class TierBuilder (desc: TierDescriptor [_, _], obj: ObjectId, gen: Long) (
    implicit scheduler: Scheduler, disks: Disks, config: StoreConfig) {

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

    val entries = new ArrayList [TierCell] (256)

    def size = entries.size

    def isEmpty = entries.isEmpty

    def last = entries.last

    var byteSize = 0

    def add (entry: TierCell, byteSize: Int) {
      entries.add (entry)
      this.byteSize += byteSize
    }}

  private val stack = new ArrayDeque [IndexNode]
  private val rstack = new ArrayDeque [IndexNode]
  private var cells = new CellsNode
  private var totalEntries = 0L
  private var totalEntryBytes = 0L
  private var totalDiskBytes = 0L

  private def push (key: Bytes, pos: Position, height: Int) {
    val node = new IndexNode (height)
    val entry = IndexEntry (key, pos)
    node.add (entry, entry.byteSize)
    stack.push (node)
  }

  private def rpush (key: Bytes, pos: Position, height: Int) {
    val node = new IndexNode (height)
    val entry = IndexEntry (key, pos)
    node.add (entry, entry.byteSize)
    rstack.push (node)
  }

  private def rpop() {
    while (!rstack.isEmpty)
      stack.push (rstack.pop())
  }

  private def add (key: Bytes, pos: Position, height: Int): Async [Unit] =
    guard {

      totalDiskBytes += pos.length

      val node = stack.peek

      if (stack.isEmpty || height < node.height) {
        push (key, pos, height)
        rpop()
        supply()

      } else {

        val entry = IndexEntry (key, pos)
        val entryByteSize = entry.byteSize

        // Ensure that an index page has at least two entries.
        if (node.byteSize + entryByteSize < config.targetPageBytes || node.size < 2) {
          node.add (entry, entryByteSize)
          rpop()
          supply()

        } else {
          stack.pop()
          val page = IndexPage (node.entries)
          val last = page.last
          for {
            pos2 <- pager.write (obj, gen, page)
            _ = rpush (key, pos, height)
            _ <- add (last.key, pos2, height+1)
          } yield ()
        }}}

  def add (key: Bytes, value: Option [Bytes]): Async [Unit] =
    guard {

      val cell = TierCell (key, value)
      val cellByteSize = cell.byteSize

      // Require that user adds entries in sorted order.
      require (cells.isEmpty || cells.last < cell)

      totalEntries += 1
      totalEntryBytes += cell.byteSize

      // Ensure that a value page has at least one entry.
      if (cells.byteSize + cellByteSize < config.targetPageBytes || cells.size < 1) {
        cells.add (cell, cellByteSize)
        supply()

      } else {
        val page = TierCellPage (cells.entries)
        cells = new CellsNode
        cells.add (cell, cellByteSize)
        val last = page.last
        for {
          pos <- pager.write (obj, gen, page)
          _ <- add (last.key, pos, 0)
        } yield ()
      }}

  private def pop (page: TierCellPage, pos0: Position): Async [Position] = {
    val node = stack.pop()
    for {
      pos1 <-
        if (node.size > 1) {
          val page = IndexPage (node.entries)
          pager.write (obj, gen, page)
        } else {
          supply (pos0)
        }
      _ <-
        if (!stack.isEmpty)
          add (page.last.key, pos1, node.height+1)
        else
          supply (totalDiskBytes += pos1.length)
    } yield pos1
  }

  def result(): Async [Tier] = {
    val page = TierCellPage (cells.entries)
    var pos: Position = null
    for {
      _ <- pager.write (obj, gen, page) .map (pos = _)
      _ <- when (cells.size > 0) (add (page.last.key, pos, 0))
      _ <- whilst (!stack.isEmpty) (pop (page, pos) .map (pos = _))
    } yield Tier (gen, pos, totalEntries, totalEntryBytes, totalDiskBytes)
  }}

private object TierBuilder {

  def build [K, V] (desc: TierDescriptor [K, V], obj: ObjectId, gen: Long, iter: TierCellIterator) (
      implicit scheduler: Scheduler, disks: Disks, config: StoreConfig): Async [Tier] = {
    val builder = new TierBuilder (desc, obj, gen)
    for {
      _ <- iter.foreach (cell => builder.add (cell.key, cell.value))
      tier <- builder.result()
    } yield tier
  }}
