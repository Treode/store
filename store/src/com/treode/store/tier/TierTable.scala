package com.treode.store.tier

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.disk.Disks
import com.treode.store.{Bytes, StoreConfig, StorePicklers}

import Async.async

trait TierTable {

  def ceiling (key: Bytes, limit: Bytes): Async [Cell]

  def get (key: Bytes): Async [Option [Bytes]]

  def iterator: CellIterator

  def put (key: Bytes, value: Bytes): Long

  def delete (key: Bytes): Long

  def checkpoint(): Async [TierTable.Meta]
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
    }}

  def apply (desc: TierDescriptor [_, _]) (
      implicit scheduler: Scheduler, disk: Disks, config: StoreConfig): TierTable =
    SynthTable (desc)
}
