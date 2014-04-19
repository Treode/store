package com.treode.store.tier

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.disk.{Disks, ObjectId, TypeId}
import com.treode.store.{Bytes, Cell, CellIterator, StoreConfig, StorePicklers, TxClock}

import Async.async
import TierTable.Meta

trait TierTable {

  def typ: TypeId

  def obj: ObjectId

  def get (key: Bytes, time: TxClock): Async [Cell]

  def iterator: CellIterator

  def iterator (key: Bytes, time: TxClock): CellIterator

  def put (key: Bytes, time: TxClock, value: Bytes): Long

  def delete (key: Bytes, time: TxClock): Long

  def receive (cells: Seq [Cell]): (Long, Seq [Cell])

  def probe (groups: Set [Long]): Set [Long]

  def compact()

  def compact (groups: Set [Long]) (p: Cell => Boolean): Async [Meta]

  def checkpoint(): Async [Meta]
}

object TierTable {

  class Meta (
      private [tier] val gen: Long,
      private [tier] val tiers: Tiers) {

    override def toString: String =
      s"TierTable.Meta($gen, $tiers)"
  }

  object Meta {

    val pickler = {
      import StorePicklers._
      wrap (ulong, Tiers.pickler)
      .build (v => new Meta (v._1, v._2))
      .inspect (v => (v.gen, v.tiers))
    }}

  def apply (desc: TierDescriptor, obj: ObjectId) (
      implicit scheduler: Scheduler, disk: Disks, config: StoreConfig): TierTable =
    SynthTable (desc, obj)
}
