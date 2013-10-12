package com.treode.store.simple

import java.util.{Arrays, ArrayList}
import com.treode.pickle.{Pickler, Picklers, PickleContext, UnpickleContext}
import com.treode.store.{Bytes, TxClock}
import com.treode.store.log.{Block, readKey, writeKey}

private class IndexBlock (val entries: Array [IndexEntry]) extends Block {

  def get (i: Int): IndexEntry =
    entries (i)

  def find (key: Bytes): Int = {
    val i = Arrays.binarySearch (entries, IndexEntry (key, 0), IndexEntry)
    if (i < 0) -i-1 else i
  }

  def size: Int = entries.size

  def isEmpty: Boolean = entries.size == 0

  def last: IndexEntry = entries (entries.size - 1)
}

private object IndexBlock {

  val empty = new IndexBlock (new Array (0))

  def apply (entries: Array [IndexEntry]): IndexBlock =
    new IndexBlock (entries)

  def apply (entries: ArrayList [IndexEntry]): IndexBlock =
    new IndexBlock (entries.toArray (empty.entries))

  private val _pickle: Pickler [IndexBlock] =
    new Pickler [IndexBlock] {

      private [this] val blockSize = Picklers.unsignedInt
      private [this] val blockPos = Picklers.unsignedLong

      // Write first entry; write full key.
      private [this] def writeEntry (entry: IndexEntry, ctx: PickleContext) {
        writeKey (entry.key, ctx)
        blockPos.p (entry.pos, ctx)
      }

      // Read first entry; read full byte array.
      private [this] def readEntry (ctx: UnpickleContext): IndexEntry = {
        val key = readKey (ctx)
        val pos = blockPos.u (ctx)
        IndexEntry (key, pos)
      }

      // Write subsequent entry; skip common prefix of previous key.
      private [this] def writeEntry (prev: IndexEntry, entry: IndexEntry, ctx: PickleContext) {
        writeKey (prev.key, entry.key, ctx)
        blockPos.p (entry.pos, ctx)
      }

      // Read subsequent entry, use common prefix from previous key.
      private [this] def readEntry (prev: IndexEntry, ctx: UnpickleContext): IndexEntry = {
        val key = readKey (prev.key, ctx)
        val pos = blockPos.u (ctx)
        IndexEntry (key, pos)
      }

      def p (block: IndexBlock, ctx: PickleContext) {
        blockSize.p (block.size, ctx)
        if (block.size > 0) {
          var prev = block.get (0)
          writeEntry (prev, ctx)
          var i = 1
          while (i < block.size) {
            val next = block.get (i)
            writeEntry (prev, next, ctx)
            prev = next
            i += 1
          }}}

      def u (ctx: UnpickleContext): IndexBlock = {
        val size = blockSize.u (ctx)
        val entries = new Array [IndexEntry] (size)
        if (size > 0) {
          var prev = readEntry (ctx)
          entries (0) = prev
          var i = 1
          while (i < size) {
            prev = readEntry (prev, ctx)
            entries (i) = prev
            i += 1
          }}
        new IndexBlock (entries)
      }}

  val pickle = {
    import Picklers._
    tagged [IndexBlock] (0x1 -> _pickle)
  }}
