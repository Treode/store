package com.treode.store.simple

import com.treode.async.Callback
import com.treode.disk.{PageDescriptor, TypeId}
import com.treode.store.{Bytes, StorePicklers}

trait SimpleTable {

  def get (key: Bytes, cb: Callback [Option [Bytes]])

  def iterator (cb: Callback [SimpleIterator])

  def put (key: Bytes, value: Bytes): Long

  def delete (key: Bytes): Long

  def checkpoint (cb: Callback [SimpleTable.Meta])
}

object SimpleTable {

  class Meta (
      private [simple] val gen: Long,
      private [simple] val tiers: Tiers)

  object Meta {

    val pickler = {
      import StorePicklers._
      wrap (ulong, Tiers.pickler)
      .build (v => new Meta (v._1, v._2))
      .inspect (v => (v.gen, v.tiers))
    }}}
