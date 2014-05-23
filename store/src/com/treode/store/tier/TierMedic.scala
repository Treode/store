package com.treode.store.tier

import com.treode.async.Scheduler
import com.treode.disk.Disk
import com.treode.store.{Bytes, Cell, StoreConfig, TableId, TxClock}

private [store] trait TierMedic {

  def put (gen: Long, key: Bytes, time: TxClock, value: Bytes)

  def delete (gen: Long, key: Bytes, time: TxClock)

  def receive (gen: Long, novel: Seq [Cell])

  def checkpoint (meta: TierTable.Meta)

  def close () (implicit launch: Disk.Launch): TierTable
}

private [store] object TierMedic {

  def apply (
      desc: TierDescriptor,
      id: TableId
  ) (implicit
      scheduler: Scheduler,
      config: StoreConfig
  ): TierMedic =
    new SynthMedic (desc, id)
}
