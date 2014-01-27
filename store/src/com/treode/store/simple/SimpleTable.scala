package com.treode.store.simple

import com.treode.async.Callback
import com.treode.store.{Bytes, StorePicklers}

trait SimpleTable {

  def get (key: Bytes, cb: Callback [Option [Bytes]])

  def iterator (cb: Callback [SimpleIterator])

  def put (key: Bytes, value: Bytes): Long

  def delete (key: Bytes): Long

  def checkpoint (cb: Callback [SimpleTable.Meta])
}

object SimpleTable {

  case class Meta (gen: Long, tiers: Tiers)

  object Meta {

    val pickler = {
      import StorePicklers._
      wrap (long, array (pos))
      .build ((Meta.apply _).tupled)
      .inspect (v => (v.gen, v.tiers))
    }}}
