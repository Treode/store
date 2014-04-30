package com.treode.store.tier

import com.treode.async.Scheduler
import com.treode.disk.Disks
import com.treode.store.{Bytes, StoreConfig, TableId, TxClock}

private [store] trait TierMedic {

  def put (gen: Long, key: Bytes, time: TxClock, value: Bytes)

  def delete (gen: Long, key: Bytes, time: TxClock)

  def checkpoint (meta: TierTable.Meta)

  def close () (implicit launch: Disks.Launch): TierTable
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
