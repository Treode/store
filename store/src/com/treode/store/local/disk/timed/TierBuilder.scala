package com.treode.store.local.disk.timed

import java.util.{ArrayDeque, ArrayList}
import com.treode.async.{Callback, delay}
import com.treode.disk.{Disks, Position}
import com.treode.store.{Bytes, StoreConfig, TimedCell, TxClock}

private class TierBuilder (config: StoreConfig) (implicit val disks: Disks) {

  private def newIndexEntries = new ArrayList [IndexEntry] (1024)
  private def newCellEntries = new ArrayList [TimedCell] (256)

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

  private def add (key: Bytes, time: TxClock, pos: Position, height: Int, cb: Callback [Unit]) {

    val node = stack.peek

    if (stack.isEmpty || height < node.height) {
      push (key, time, pos, height)
      rpop()
      cb()

    } else {

      val entry = IndexEntry (key, time, pos)
      val entryByteSize = entry.byteSize

      // Ensure that an index page has at least two entries.
      if (node.byteSize + entryByteSize < config.targetPageBytes || node.size < 2) {
        node.add (entry, entryByteSize)
        rpop()
        cb()

      } else {
        stack.pop()
        val page = IndexPage (node.entries)
        val last = page.last
        TierPage.page.write (0, page, delay (cb) { pos2 =>
          rpush (key, time, pos, height)
          add (last.key, last.time, pos2, height+1, cb)
        })
      }}}

  def add (key: Bytes, time: TxClock, value: Option [Bytes], cb: Callback [Unit]) {

    val entry = TimedCell (key, time, value)
    val entryByteSize = entry.byteSize

    // Require that user adds entries in sorted order.
    require (entries.isEmpty || entries.get (entries.size-1) < entry)

    // Ensure that a value page has at least one entry.
    if (byteSize + entryByteSize < config.targetPageBytes || entries.size < 1) {
      entries.add (entry)
      byteSize += entryByteSize
      cb()

    } else {
      val page = CellPage (entries)
      entries = newCellEntries
      entries.add (entry)
      byteSize = entryByteSize
      val last = page.last
      TierPage.page.write (0, page, delay (cb) { pos =>
        add (last.key, last.time, pos, 0, cb)
      })
    }}

  def result (cb: Callback [Position]) {

    val page = CellPage (entries)
    val last = page.last
    TierPage.page.write (0, page, delay (cb) { _pos =>

      var pos = _pos

      if (stack.isEmpty) {
        cb (pos)

      } else {

        val loop = new Callback [Unit] {

          def next (node: IndexNode) {
            if (!stack.isEmpty)
              add (last.key, last.time, pos, node.height+1, this)
            else
              cb (pos)
          }

          def pass (v: Unit) {
            val node = stack.pop()
            if (node.size > 1) {
              val page = IndexPage (node.entries)
              TierPage.page.write (0, page, delay (cb) { _pos =>
                pos = _pos
                next (node)
              })
            } else {
              next (node)
            }}

          def fail (t: Throwable) = cb.fail (t)
        }

        add (last.key, last.time, pos, 0, loop)

      }})
  }}
