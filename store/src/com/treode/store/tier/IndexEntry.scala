package com.treode.store.tier

import com.google.common.primitives.Longs
import com.treode.pickle.size
import com.treode.store.{Bytes, StorePicklers, TxClock}

private class IndexEntry (val key: Bytes, val time: TxClock, val pos: Long)
extends Ordered [IndexEntry] {

  def byteSize = size (IndexEntry.pickle, this)

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

  val pickle = {
    import StorePicklers._
    wrap3 (bytes, txClock, long) (IndexEntry.apply _) (v => (v.key, v.time, v.pos))
  }}
