package com.treode.store.simple

import com.google.common.primitives.Longs
import com.treode.pickle.size
import com.treode.store.{Bytes, StorePicklers, TxClock}
import com.treode.disk.Position

private class IndexEntry (val key: Bytes, val disk: Int, val offset: Long, val length: Int)
extends Ordered [IndexEntry] {

  def pos = Position (disk, offset, length)

  def byteSize = size (IndexEntry.pickle, this)

  def compare (that: IndexEntry): Int = key compare that.key

  override def toString = s"IndexEntry($key,$disk,$offset,$length)"
}

private object IndexEntry extends Ordering [IndexEntry] {

  def apply (key: Bytes, disk: Int, offset: Long, length: Int): IndexEntry =
    new IndexEntry (key, disk, offset, length)

  def apply (key: Bytes, pos: Position): IndexEntry =
    new IndexEntry (key, pos.disk, pos.offset, pos.length)

  def compare (x: IndexEntry, y: IndexEntry): Int =
    x compare y

  val pickle = {
    import StorePicklers._
    wrap (bytes, uint, long, uint)
    .build (v => IndexEntry (v._1, v._2, v._3, v._4))
    .inspect (v => (v.key, v.disk, v.offset, v.length))
  }}
