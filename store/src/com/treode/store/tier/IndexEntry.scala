package com.treode.store.tier

import com.treode.store.{Bytes, TxClock}

private class IndexEntry (val key: Bytes, val time: TxClock, val pos: Long)
extends Entry with Ordered [IndexEntry] {

  def compare (that: IndexEntry): Int = {
    val r = key compare that.key
    if (r != 0)
      return r
    // Reverse chronological order
    that.time compare time
  }

  override def toString = "IndexEntry" + (key, time, pos)
}

private object IndexEntry extends Ordering [IndexEntry] {

  def apply (key: Bytes, time: TxClock, pos: Long): IndexEntry =
    new IndexEntry (key, time, pos)

  def compare (x: IndexEntry, y: IndexEntry): Int =
    x compare y
}
