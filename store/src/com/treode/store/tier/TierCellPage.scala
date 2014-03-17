package com.treode.store.tier

import java.util.{Arrays, ArrayList}

import com.treode.pickle.{Pickler, Picklers, PickleContext, UnpickleContext}
import com.treode.store.{Bytes, TxClock}

private class TierCellPage (val entries: Array [TierCell]) extends TierPage {

  def get (i: Int): TierCell =
    entries (i)

  def ceiling (key: Bytes): Int = {
    val i = Arrays.binarySearch (entries, TierCell (key, None), TierCell)
    if (i < 0) -i-1 else i
  }

  def size: Int = entries.size

  def isEmpty: Boolean = entries.size == 0

  def last: TierCell = entries (entries.size - 1)

  override def toString =
    s"CellPage(${entries.head.key}, ${entries.last.key})"
}

private object TierCellPage {

  val empty = new TierCellPage (new Array (0))

  def apply (entries: Array [TierCell]): TierCellPage =
    new TierCellPage (entries)

  def apply (entries: ArrayList [TierCell]): TierCellPage =
    new TierCellPage (entries.toArray (empty.entries))

  val pickler: Pickler [TierCellPage] =
    new AbstractPagePickler [TierCellPage, TierCell] {

      private [this] val value = Picklers.option (Bytes.pickler)

      protected def writeEntry (entry: TierCell, ctx: PickleContext) {
        writeKey (entry.key, ctx)
        value.p (entry.value, ctx)
      }

      protected def readEntry (ctx: UnpickleContext): TierCell =
        TierCell (readKey (ctx), value.u (ctx))

      protected def writeEntry (prev: TierCell, entry: TierCell, ctx: PickleContext) {
        writeKey (prev.key, entry.key, ctx)
        value.p (entry.value, ctx)
      }

      protected def readEntry (prev: TierCell, ctx: UnpickleContext): TierCell =
        TierCell (readKey (prev.key, ctx), value.u (ctx))

      def p (page: TierCellPage, ctx: PickleContext): Unit =
        _p (page.entries, ctx)

      def u (ctx: UnpickleContext): TierCellPage =
        new TierCellPage (_u (ctx))
  }}
