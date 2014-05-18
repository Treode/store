package com.treode.store.tier

import com.treode.async.{Async, Callback, Scheduler}
import com.treode.disk.{Disk, TypeId}
import com.treode.store._

import Async.async
import TierTable.Meta

private [store] trait TierTable {

  def typ: TypeId

  def id: TableId

  def get (key: Bytes, time: TxClock): Async [Cell]

  def iterator (residents: Residents): CellIterator

  def iterator (key: Bytes, time: TxClock, residents: Residents): CellIterator

  def put (key: Bytes, time: TxClock, value: Bytes): Long

  def delete (key: Bytes, time: TxClock): Long

  def receive (cells: Seq [Cell]): (Long, Seq [Cell])

  def probe (groups: Set [Long]): Async [Set [Long]]

  def compact()

  def compact (groups: Set [Long], residents: Residents): Async [Meta]

  def checkpoint (residents: Residents): Async [Meta]
}

private [store] object TierTable {

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

  def apply (desc: TierDescriptor, id: TableId) (
      implicit scheduler: Scheduler, disk: Disk, config: StoreConfig): TierTable =
    SynthTable (desc, id)
}
