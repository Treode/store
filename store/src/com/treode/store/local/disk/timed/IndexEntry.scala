package com.treode.store.local.disk.timed

import com.google.common.primitives.Longs
import com.treode.pickle.size
import com.treode.store.{Bytes, StorePicklers, TxClock}
import com.treode.disk.Position

private class IndexEntry (val key: Bytes, val time: TxClock, val disk: Int, val offset: Long,
    val length: Int) extends Ordered [IndexEntry] {

  def pos = Position (disk, offset, length)

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

  def apply (key: Bytes, time: TxClock, disk: Int, offset: Long, length: Int): IndexEntry =
    new IndexEntry (key, time, disk, offset, length)

  def apply (key: Bytes, time: TxClock, pos: Position): IndexEntry =
    new IndexEntry (key, time, pos.disk, pos.offset, pos.length)

  def compare (x: IndexEntry, y: IndexEntry): Int =
    x compare y

  val pickle = {
    import StorePicklers._
    wrap (bytes, txClock, uint, ulong, uint)
    .build (v => (IndexEntry (v._1, v._2, v._3, v._4, v._5)))
    .inspect (v => (v.key, v.time, v.disk, v.offset, v.length))
  }}
