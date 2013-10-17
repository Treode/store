package com.treode.store.tier

import java.util.{Arrays, ArrayList}
import com.treode.pickle.{Pickler, Picklers, PickleContext, UnpickleContext}
import com.treode.store.{Bytes, TxClock}
import com.treode.store.disk.{AbstractBlockPickler, Block}

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
    new AbstractBlockPickler [CellBlock, Cell] {

      private [this] val value = Picklers.option (Bytes.pickle)
      private [this] val txClock = TxClock.pickle

      protected def writeEntry (entry: Cell, ctx: PickleContext) {
        writeKey (entry.key, ctx)
        txClock.p (entry.time, ctx)
        value.p (entry.value, ctx)
      }

      protected def readEntry (ctx: UnpickleContext): Cell =
        Cell (readKey (ctx), txClock.u (ctx), value.u (ctx))

      protected def writeEntry (prev: Cell, entry: Cell, ctx: PickleContext) {
        writeKey (prev.key, entry.key, ctx)
        txClock.p (entry.time, ctx)
        value.p (entry.value, ctx)
      }

      protected def readEntry (prev: Cell, ctx: UnpickleContext): Cell =
        Cell (readKey (prev.key, ctx), txClock.u (ctx), value.u (ctx))

      def p (block: CellBlock, ctx: PickleContext): Unit =
        _p (block.entries, ctx)

      def u (ctx: UnpickleContext): CellBlock =
        new CellBlock (_u (ctx))
    }

  val pickle = {
    import Picklers._
    tagged [CellBlock] (0x1 -> _pickle)
  }}
