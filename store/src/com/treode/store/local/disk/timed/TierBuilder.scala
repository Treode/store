package com.treode.store.local.disk.timed

import java.util.{ArrayDeque, ArrayList}
import com.treode.concurrent.Callback
import com.treode.store.{Bytes, TimedCell, TxClock}
import com.treode.store.local.disk.DiskSystem

private class TierBuilder (disk: DiskSystem) {

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

  private def push (key: Bytes, time: TxClock, pos: Long, height: Int) {
    val node = new IndexNode (height)
    val entry = IndexEntry (key, time, pos)
    node.add (entry, entry.byteSize)
    stack.push (node)
  }

  private def rpush (key: Bytes, time: TxClock, pos: Long, height: Int) {
    val node = new IndexNode (height)
    val entry = IndexEntry (key, time, pos)
    node.add (entry, entry.byteSize)
    rstack.push (node)
  }

  private def rpop() {
    while (!rstack.isEmpty)
      stack.push (rstack.pop())
  }

  private def add (key: Bytes, time: TxClock, pos: Long, height: Int, cb: Callback [Unit]) {

    val node = stack.peek

    if (stack.isEmpty || height < node.height) {
      push (key, time, pos, height)
      rpop()
      cb()

    } else {

      val entry = IndexEntry (key, time, pos)
      val entryByteSize = entry.byteSize

      // Ensure that an index page has at least two entries.
      if (node.byteSize + entryByteSize < disk.maxPageSize || node.size < 2) {
        node.add (entry, entryByteSize)
        rpop()
        cb()

      } else {
        stack.pop()
        val page = IndexPage (node.entries)
        val last = page.last
        val pos2 = disk.write (page, new Callback [Long] {
          def pass (pos2: Long) {
            rpush (key, time, pos, height)
            add (last.key, last.time, pos2, height+1, cb)
          }
          def fail (t: Throwable) = cb.fail (t)
        })
      }}}

  def add (key: Bytes, time: TxClock, value: Option [Bytes], cb: Callback [Unit]) {

    val entry = TimedCell (key, time, value)
    val entryByteSize = entry.byteSize

    // Require that user adds entries in sorted order.
    require (entries.isEmpty || entries.get (entries.size-1) < entry)

    // Ensure that a value page has at least one entry.
    if (byteSize + entryByteSize < disk.maxPageSize || entries.size < 1) {
      entries.add (entry)
      byteSize += entryByteSize
      cb()

    } else {
      val page = CellPage (entries)
      entries = newCellEntries
      entries.add (entry)
      byteSize = entryByteSize
      val last = page.last
      disk.write (page, new Callback [Long] {
        def pass (pos: Long): Unit = add (last.key, last.time, pos, 0, cb)
        def fail (t: Throwable) = cb.fail (t)
      })
    }}

  def result (cb: Callback [Long]) {

    val page = CellPage (entries)
    val last = page.last
    disk.write (page, new Callback [Long] {

      def pass (_pos: Long) {

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
              if (node.size > 1)
                disk.write (IndexPage (node.entries), new Callback [Long] {
                  def pass (_pos: Long) {
                    pos = _pos
                    next (node)
                  }
                  def fail (t: Throwable) = cb.fail (t)
                })
              else
                next (node)
            }

            def fail (t: Throwable) = cb.fail (t)
          }

          add (last.key, last.time, pos, 0, loop)
        }}

      def fail (t: Throwable) = cb.fail (t)
    })
  }}
