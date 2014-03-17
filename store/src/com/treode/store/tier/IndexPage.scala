package com.treode.store.tier

import java.util.{Arrays, ArrayList}

import com.treode.pickle.{Pickler, Picklers, PickleContext, UnpickleContext}
import com.treode.store.{Bytes, TxClock}

private class IndexPage (val entries: Array [IndexEntry]) extends TierPage {

  def get (i: Int): IndexEntry =
    entries (i)

  def ceiling (key: Bytes): Int = {
    val i = Arrays.binarySearch (entries, IndexEntry (key, 0, 0, 0), IndexEntry)
    if (i < 0) -i-1 else i
  }

  def size: Int = entries.size

  def isEmpty: Boolean = entries.size == 0

  def last: IndexEntry = entries (entries.size - 1)

  override def toString =
    s"IndexPage(${entries.head.key}, ${entries.last.key})"
}

private object IndexPage {

  val empty = new IndexPage (new Array (0))

  def apply (entries: Array [IndexEntry]): IndexPage =
    new IndexPage (entries)

  def apply (entries: ArrayList [IndexEntry]): IndexPage =
    new IndexPage (entries.toArray (empty.entries))

  val pickler: Pickler [IndexPage] =
    new AbstractPagePickler [IndexPage, IndexEntry] {

      private [this] val disk = Picklers.uint
      private [this] val offset = Picklers.ulong
      private [this] val length = Picklers.uint

      protected def writeEntry (entry: IndexEntry, ctx: PickleContext) {
        writeKey (entry.key, ctx)
        disk.p (entry.disk, ctx)
        offset.p (entry.offset, ctx)
        length.p (entry.length, ctx)
      }

      protected def readEntry (ctx: UnpickleContext): IndexEntry = {
        val key = readKey (ctx)
        val _disk = disk.u (ctx)
        val _offset = offset.u (ctx)
        val _length = length.u (ctx)
        IndexEntry (key, _disk, _offset, _length)
      }

      protected def writeEntry (prev: IndexEntry, entry: IndexEntry, ctx: PickleContext) {
        writeKey (prev.key, entry.key, ctx)
        disk.p (entry.disk, ctx)
        offset.p (entry.offset, ctx)
        length.p (entry.length, ctx)
      }

      protected def readEntry (prev: IndexEntry, ctx: UnpickleContext): IndexEntry = {
        val key = readKey (prev.key, ctx)
        val _disk = disk.u (ctx)
        val _offset = offset.u (ctx)
        val _length = length.u (ctx)
        IndexEntry (key, _disk, _offset, _length)
      }

      def p (page: IndexPage, ctx: PickleContext): Unit =
        _p (page.entries, ctx)

      def u (ctx: UnpickleContext): IndexPage =
        new IndexPage (_u (ctx))
  }}
