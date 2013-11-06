package com.treode.store.local.disk.timed

import java.util.{Arrays, ArrayList}
import com.treode.pickle.{Pickler, Picklers, PickleContext, UnpickleContext}
import com.treode.store.{Bytes, TxClock}
import com.treode.store.local.TimedCell
import com.treode.store.local.disk.{AbstractPagePickler, Page}

private class CellPage (val entries: Array [TimedCell]) extends Page {

  def get (i: Int): TimedCell =
    entries (i)

  def find (key: Bytes, time: TxClock): Int = {
    val i = Arrays.binarySearch (entries, TimedCell (key, time, None), TimedCell)
    if (i < 0) -i-1 else i
  }

  def size: Int = entries.size

  def isEmpty: Boolean = entries.size == 0

  def last: TimedCell = entries (entries.size - 1)
}

private object CellPage {

  val empty = new CellPage (new Array (0))

  def apply (entries: Array [TimedCell]): CellPage =
    new CellPage (entries)

  def apply (entries: ArrayList [TimedCell]): CellPage =
    new CellPage (entries.toArray (empty.entries))

  private val _pickle: Pickler [CellPage] =
    new AbstractPagePickler [CellPage, TimedCell] {

      private [this] val value = Picklers.option (Bytes.pickle)
      private [this] val txClock = TxClock.pickle

      protected def writeEntry (entry: TimedCell, ctx: PickleContext) {
        writeKey (entry.key, ctx)
        txClock.p (entry.time, ctx)
        value.p (entry.value, ctx)
      }

      protected def readEntry (ctx: UnpickleContext): TimedCell =
        TimedCell (readKey (ctx), txClock.u (ctx), value.u (ctx))

      protected def writeEntry (prev: TimedCell, entry: TimedCell, ctx: PickleContext) {
        writeKey (prev.key, entry.key, ctx)
        txClock.p (entry.time, ctx)
        value.p (entry.value, ctx)
      }

      protected def readEntry (prev: TimedCell, ctx: UnpickleContext): TimedCell =
        TimedCell (readKey (prev.key, ctx), txClock.u (ctx), value.u (ctx))

      def p (page: CellPage, ctx: PickleContext): Unit =
        _p (page.entries, ctx)

      def u (ctx: UnpickleContext): CellPage =
        new CellPage (_u (ctx))
    }

  val pickle = {
    import Picklers._
    tagged [CellPage] (0x1 -> _pickle)
  }}
