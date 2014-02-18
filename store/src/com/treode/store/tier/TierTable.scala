package com.treode.store.tier

import com.treode.async.{Async, Callback}
import com.treode.disk.{PageDescriptor, TypeId}
import com.treode.store.{Bytes, StorePicklers}

import Async.async

trait TierTable {

  def get (key: Bytes, cb: Callback [Option [Bytes]])

  def iterator: CellIterator

  def put (key: Bytes, value: Bytes): Long

  def delete (key: Bytes): Long

  def checkpoint(): Async [TierTable.Meta]

  def get (key: Bytes): Async [Option [Bytes]] =
    async (get (key, _))
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
