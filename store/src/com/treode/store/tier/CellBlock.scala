package com.treode.store.tier

import java.util.{Arrays, ArrayList}

import com.treode.pickle.{Pickler, Picklers, PickleContext, UnpickleContext}
import com.treode.store.{Bytes, TxClock}

private class CellBlock (val entries: Array [Cell]) extends Block {

  def get (i: Int): Cell =
    entries (i)

  def find (key: Bytes, time: TxClock): Int = {
    val i = Arrays.binarySearch (entries, Cell (key, time, None), Cell)
    if (i < 0) -i-1 else i
  }

  def size: Int = entries.size

  def isEmpty: Boolean = entries.size == 0

  def last: Cell = entries (entries.size - 1)
}

private object CellBlock {

  val empty = new CellBlock (new Array (0))

  def apply (entries: Array [Cell]): CellBlock =
    new CellBlock (entries)

  def apply (entries: ArrayList [Cell]): CellBlock =
    new CellBlock (entries.toArray (empty.entries))

  private val _pickle: Pickler [CellBlock] =
    new Pickler [CellBlock] {

      private [this] val blockSize = Picklers.unsignedInt
      private [this] val value = Picklers.option (Bytes.pickle)
      private [this] val txClock = TxClock.pickle

      // Write first entry; write full key.
      private [this] def writeEntry (entry: Cell, ctx: PickleContext) {
        writeKey (entry.key, ctx)
        txClock.p (entry.time, ctx)
        value.p (entry.value, ctx)
      }

      // Read first entry; read full byte array.
      private [this] def readEntry (ctx: UnpickleContext): Cell =
        Cell (readKey (ctx), txClock.u (ctx), value.u (ctx))

      // Write subsequent entry; skip common prefix of previous key.
      private [this] def writeEntry (prev: Cell, entry: Cell, ctx: PickleContext) {
        writeKey (prev.key, entry.key, ctx)
        txClock.p (entry.time, ctx)
        value.p (entry.value, ctx)
      }

      // Read subsequent entry, use common prefix from previous key.
      private [this] def readEntry (prev: Cell, ctx: UnpickleContext): Cell =
        Cell (readKey (prev.key, ctx), txClock.u (ctx), value.u (ctx))

      def p (block: CellBlock, ctx: PickleContext) {
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

      def u (ctx: UnpickleContext): CellBlock = {
        val size = blockSize.u (ctx)
        val entries = new Array [Cell] (size)
        if (size > 0) {
          var prev = readEntry (ctx)
          entries (0) = prev
          var i = 1
          while (i < size) {
            prev = readEntry (prev, ctx)
            entries (i) = prev
            i += 1
          }}
        new CellBlock (entries)
      }}

  val pickle = {
    import Picklers._
    tagged [CellBlock] (0x1 -> _pickle)
  }}
