package com.treode.store.tier

import java.util.Arrays

import com.treode.pickle.{Pickler, Picklers, PickleContext, UnpickleContext}
import com.treode.store.{Bytes, TxClock}

private class ValueBlock (val entries: Array [ValueEntry]) extends Block {

  def get (i: Int): ValueEntry =
    entries (i)

  def find (key: Bytes, time: TxClock): Int = {
    val i = Arrays.binarySearch (entries, ValueEntry (key, time, None), ValueEntry)
    if (i < 0) -i-1 else i
  }

  def size: Int = entries.size
}

private object ValueBlock {

  val empty = new ValueBlock (new Array (0))

  private val _pickle: Pickler [ValueBlock] =
    new Pickler [ValueBlock] {

      private [this] val blockSize = Picklers.unsignedInt
      private [this] val value = Picklers.option (Bytes.pickle)
      private [this] val txClock = TxClock.pickle

      // Write first entry; write full key.
      private [this] def writeEntry (entry: ValueEntry, ctx: PickleContext) {
        writeKey (entry.key, ctx)
        txClock.p (entry.time, ctx)
        value.p (entry.value, ctx)
      }

      // Read first entry; read full byte array.
      private [this] def readEntry (ctx: UnpickleContext): ValueEntry =
        ValueEntry (readKey (ctx), txClock.u (ctx), value.u (ctx))

      // Write subsequent entry; skip common prefix of previous key.
      private [this] def writeEntry (prev: ValueEntry, entry: ValueEntry, ctx: PickleContext) {
        writeKey (prev.key, entry.key, ctx)
        txClock.p (entry.time, ctx)
        value.p (entry.value, ctx)
      }

      // Read subsequent entry, use common prefix from previous key.
      private [this] def readEntry (prev: ValueEntry, ctx: UnpickleContext): ValueEntry =
        ValueEntry (readKey (prev.key, ctx), txClock.u (ctx), value.u (ctx))

      def p (block: ValueBlock, ctx: PickleContext) {
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

      def u (ctx: UnpickleContext): ValueBlock = {
        val size = blockSize.u (ctx)
        val entries = new Array [ValueEntry] (size)
        if (size > 0) {
          var prev = readEntry (ctx)
          entries (0) = prev
          var i = 1
          while (i < size) {
            prev = readEntry (prev, ctx)
            entries (i) = prev
            i += 1
          }}
        new ValueBlock (entries)
      }}

  val pickle = {
    import Picklers._
    tagged [ValueBlock] (0x1 -> _pickle)
  }}
