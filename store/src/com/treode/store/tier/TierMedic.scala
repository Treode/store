package com.treode.store.tier

import com.treode.async.Scheduler
import com.treode.disk.{Disks, ObjectId}
import com.treode.store.{Bytes, StoreConfig, TxClock}

trait TierMedic {

  def put (gen: Long, key: Bytes, time: TxClock, value: Bytes)

  def delete (gen: Long, key: Bytes, time: TxClock)

  def checkpoint (meta: TierTable.Meta)

  def close () (implicit launch: Disks.Launch): TierTable
}

object TierMedic {

  def apply (
      desc: TierDescriptor,
      obj: ObjectId
  ) (implicit
      scheduler: Scheduler,
      config: StoreConfig
  ): TierMedic =
    new SynthMedic (desc, obj)
}
