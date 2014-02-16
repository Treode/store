package com.treode.store.tier

import com.treode.async.Callback
import com.treode.disk.{PageDescriptor, TypeId}
import com.treode.store.{Bytes, StorePicklers}

trait TierTable {

  def get (key: Bytes, cb: Callback [Option [Bytes]])

  def iterator (cb: Callback [CellIterator])

  def put (key: Bytes, value: Bytes): Long

  def delete (key: Bytes): Long

  def checkpoint (cb: Callback [TierTable.Meta])
}

object TierTable {

  class Meta (
      private [tier] val gen: Long,
      private [tier] val tiers: Tiers)

  object Meta {

    val pickler = {
      import StorePicklers._
      wrap (ulong, Tiers.pickler)
      .build (v => new Meta (v._1, v._2))
      .inspect (v => (v.gen, v.tiers))
    }}}
