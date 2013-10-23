package com.treode.store.simple

import java.util.{Arrays, ArrayList}
import com.treode.pickle.{Pickler, Picklers, PickleContext, UnpickleContext}
import com.treode.store.{Bytes, TxClock}
import com.treode.store.disk.{AbstractPagePickler, Page}

private class CellPage (val entries: Array [Cell]) extends Page {

  def get (i: Int): Cell =
    entries (i)

  def find (key: Bytes): Int = {
    val i = Arrays.binarySearch (entries, Cell (key, None), Cell)
    if (i < 0) -i-1 else i
  }

  def size: Int = entries.size

  def isEmpty: Boolean = entries.size == 0

  def last: Cell = entries (entries.size - 1)
}

private object CellPage {

  val empty = new CellPage (new Array (0))

  def apply (entries: Array [Cell]): CellPage =
    new CellPage (entries)

  def apply (entries: ArrayList [Cell]): CellPage =
    new CellPage (entries.toArray (empty.entries))

  private val _pickle: Pickler [CellPage] =
    new AbstractPagePickler [CellPage, Cell] {

      private [this] val value = Picklers.option (Bytes.pickle)

      protected def writeEntry (entry: Cell, ctx: PickleContext) {
        writeKey (entry.key, ctx)
        value.p (entry.value, ctx)
      }

      protected def readEntry (ctx: UnpickleContext): Cell =
        Cell (readKey (ctx), value.u (ctx))

      protected def writeEntry (prev: Cell, entry: Cell, ctx: PickleContext) {
        writeKey (prev.key, entry.key, ctx)
        value.p (entry.value, ctx)
      }

      protected def readEntry (prev: Cell, ctx: UnpickleContext): Cell =
        Cell (readKey (prev.key, ctx), value.u (ctx))

      def p (page: CellPage, ctx: PickleContext): Unit =
        _p (page.entries, ctx)

      def u (ctx: UnpickleContext): CellPage =
        new CellPage (_u (ctx))
  }

  val pickle = {
    import Picklers._
    tagged [CellPage] (0x1 -> _pickle)
  }}
