package com.treode.store.simple

import java.util.{ArrayDeque, ArrayList}
import com.treode.cluster.concurrent.Callback
import com.treode.store.{Bytes, TxClock}
import com.treode.store.disk.DiskSystem

private class TierBuilder (disk: DiskSystem) {

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

  private def push (key: Bytes, pos: Long, height: Int) {
    val node = new IndexNode (height)
    val entry = IndexEntry (key, pos)
    node.add (entry, entry.byteSize)
    stack.push (node)
  }

  private def rpush (key: Bytes, pos: Long, height: Int) {
    val node = new IndexNode (height)
    val entry = IndexEntry (key, pos)
    node.add (entry, entry.byteSize)
    rstack.push (node)
  }

  private def rpop() {
    while (!rstack.isEmpty)
      stack.push (rstack.pop())
  }

  private def add (key: Bytes, pos: Long, height: Int, cb: Callback [Unit]) {

    val node = stack.peek

    if (stack.isEmpty || height < node.height) {
      push (key, pos, height)
      rpop()
      cb()

    } else {

      val entry = IndexEntry (key, pos)
      val entryByteSize = entry.byteSize

      // Ensure that an index block has at least two entries.
      if (node.byteSize + entryByteSize < disk.maxBlockSize || node.size < 2) {
        node.add (entry, entryByteSize)
        rpop()
        cb()

      } else {
        stack.pop()
        val block = IndexBlock (node.entries)
        val last = block.last
        val pos2 = disk.write (block, new Callback [Long] {
          def apply (pos2: Long) {
            rpush (key, pos, height)
            add (last.key, pos2, height+1, cb)
          }
          def fail (t: Throwable) = cb.fail (t)
        })
      }}}

  def add (key: Bytes, value: Option [Bytes], cb: Callback [Unit]) {

    val entry = Cell (key, value)
    val entryByteSize = entry.byteSize

    // Require that user adds entries in sorted order.
    require (entries.isEmpty || entries.get (entries.size-1) < entry)

    // Ensure that a value block has at least one entry.
    if (byteSize + entryByteSize < disk.maxBlockSize || entries.size < 1) {
      entries.add (entry)
      byteSize += entryByteSize
      cb()

    } else {
      val block = CellBlock (entries)
      entries = newCellEntries
      entries.add (entry)
      byteSize = entryByteSize
      val last = block.last
      disk.write (block, new Callback [Long] {
        def apply (pos: Long): Unit = add (last.key, pos, 0, cb)
        def fail (t: Throwable) = cb.fail (t)
      })
    }}

  def result (cb: Callback [Long]) {

    val block = CellBlock (entries)
    val last = block.last
    disk.write (block, new Callback [Long] {

      def apply (_pos: Long) {

        var pos = _pos

        if (stack.isEmpty) {
          cb (pos)

        } else {

          val loop = new Callback [Unit] {

            def next (node: IndexNode) {
              if (!stack.isEmpty)
                add (last.key, pos, node.height+1, this)
              else
                cb (pos)
            }

            def apply (v: Unit) {
              val node = stack.pop()
              if (node.size > 1)
                disk.write (IndexBlock (node.entries), new Callback [Long] {
                  def apply (_pos: Long) {
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

          add (last.key, pos, 0, loop)
        }}

      def fail (t: Throwable) = cb.fail (t)
    })
  }}
