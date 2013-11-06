package com.treode.store.local.disk.simple

import java.util.{Arrays, ArrayList}
import com.treode.pickle.{Pickler, Picklers, PickleContext, UnpickleContext}
import com.treode.store.{Bytes, TxClock}
import com.treode.store.local.disk.{AbstractPagePickler, Page}

private class IndexPage (val entries: Array [IndexEntry]) extends Page {

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

private object IndexPage {

  val empty = new IndexPage (new Array (0))

  def apply (entries: Array [IndexEntry]): IndexPage =
    new IndexPage (entries)

  def apply (entries: ArrayList [IndexEntry]): IndexPage =
    new IndexPage (entries.toArray (empty.entries))

  private val _pickle: Pickler [IndexPage] =
    new AbstractPagePickler [IndexPage, IndexEntry] {

      private [this] val blockPos = Picklers.ulong

      protected def writeEntry (entry: IndexEntry, ctx: PickleContext) {
        writeKey (entry.key, ctx)
        blockPos.p (entry.pos, ctx)
      }

      protected def readEntry (ctx: UnpickleContext): IndexEntry = {
        val key = readKey (ctx)
        val pos = blockPos.u (ctx)
        IndexEntry (key, pos)
      }

      protected def writeEntry (prev: IndexEntry, entry: IndexEntry, ctx: PickleContext) {
        writeKey (prev.key, entry.key, ctx)
        blockPos.p (entry.pos, ctx)
      }

      protected def readEntry (prev: IndexEntry, ctx: UnpickleContext): IndexEntry = {
        val key = readKey (prev.key, ctx)
        val pos = blockPos.u (ctx)
        IndexEntry (key, pos)
      }

      def p (page: IndexPage, ctx: PickleContext): Unit =
        _p (page.entries, ctx)

      def u (ctx: UnpickleContext): IndexPage =
        new IndexPage (_u (ctx))
  }

  val pickle = {
    import Picklers._
    tagged [IndexPage] (0x1 -> _pickle)
  }}
