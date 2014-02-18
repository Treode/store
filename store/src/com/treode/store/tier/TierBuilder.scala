package com.treode.store.tier

import java.util.{ArrayDeque, ArrayList}
import scala.collection.JavaConversions._

import com.treode.async.{Async, Scheduler}
import com.treode.disk.{Disks, Position}
import com.treode.store.{Bytes, StoreConfig, TxClock}

import Async.{async, cond, guard, supply, whilst}

private class TierBuilder (desc: TierDescriptor [_, _], generation: Long) (
    implicit scheduler: Scheduler, disks: Disks, config: StoreConfig) {

  import desc.pager

  private def newIndexEntries = new ArrayList [IndexEntry] (1024)
  private def newCellEntries = new ArrayList [Cell] (256)

  private class IndexNode (val height: Int) {

    val entries = newIndexEntries

    def size = entries.size

    var byteSize = 0

    def add (entry: IndexEntry, byteSize: Int) {
      entries.add (entry)
      this.byteSize += byteSize
    }}

  private val stack = new ArrayDeque [IndexNode]
  private val rstack = new ArrayDeque [IndexNode]
  private var entries = newCellEntries
  private var byteSize = 0

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

      val node = stack.peek

      if (stack.isEmpty || height < node.height) {
        push (key, pos, height)
        rpop()
        Async.supply ()

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
            pos2 <- pager.write (generation, page)
            _ = rpush (key, pos, height)
            _ <- add (last.key, pos2, height+1)
          } yield ()
        }}}

  def add (key: Bytes, value: Option [Bytes]): Async [Unit] =
    guard {

      val entry = Cell (key, value)
      val entryByteSize = entry.byteSize

      // Require that user adds entries in sorted order.
      require (entries.isEmpty || entries.last < entry)

      // Ensure that a value page has at least one entry.
      if (byteSize + entryByteSize < config.targetPageBytes || entries.size < 1) {
        entries.add (entry)
        byteSize += entryByteSize
        supply()

      } else {
        val page = CellPage (entries)
        entries = newCellEntries
        entries.add (entry)
        byteSize = entryByteSize
        val last = page.last
        for {
          pos <- pager.write (generation, page)
          _ <- add (last.key, pos, 0)
        } yield ()
      }}

  private def pop (page: CellPage, pos0: Position): Async [Position] = {
    val node = stack.pop()
    for {
      pos1 <-
        if (node.size > 1) {
          val page = IndexPage (node.entries)
          pager.write (generation, page)
        } else {
          supply (pos0)
        }
      _ <-
        if (!stack.isEmpty)
          add (page.last.key, pos1, node.height+1)
        else
          supply()
    } yield pos1
  }

  def result(): Async [Tier] = {
    val page = CellPage (entries)
    var pos: Position = null
    for {
      _ <- pager.write (generation, page) .map (pos = _)
      _ <- cond (entries.size > 0) (add (page.last.key, pos, 0))
      _ <- whilst (!stack.isEmpty) (pop (page, pos) .map (pos = _))
    } yield Tier (generation, pos)
  }}

private object TierBuilder {

  def build [K, V] (desc: TierDescriptor [K, V], generation: Long, iter: CellIterator) (
      implicit scheduler: Scheduler, disks: Disks, config: StoreConfig): Async [Tier] = {
    val builder = new TierBuilder (desc, generation)
    for {
      _ <- iter.foreach (cell => builder.add (cell.key, cell.value))
      tier <- builder.result()
    } yield tier
  }}
