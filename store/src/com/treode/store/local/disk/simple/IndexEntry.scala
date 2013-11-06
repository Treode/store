package com.treode.store.local.disk.simple

import com.google.common.primitives.Longs
import com.treode.pickle.size
import com.treode.store.{Bytes, StorePicklers, TxClock}

private class IndexEntry (val key: Bytes, val pos: Long) extends Ordered [IndexEntry] {

  def byteSize = size (IndexEntry.pickle, this)

  def compare (that: IndexEntry): Int = key compare that.key

  override def toString = "IndexEntry" + (key, pos)
}

private object IndexEntry extends Ordering [IndexEntry] {

  def apply (key: Bytes, pos: Long): IndexEntry =
    new IndexEntry (key, pos)

  def compare (x: IndexEntry, y: IndexEntry): Int =
    x compare y

  val pickle = {
    import StorePicklers._
    wrap2 (bytes, long) (IndexEntry.apply _) (v => (v.key, v.pos))
  }}
