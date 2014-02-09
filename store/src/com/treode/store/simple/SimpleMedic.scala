package com.treode.store.simple

import com.treode.async.Scheduler
import com.treode.disk.{Launch, Recovery, TypeId}
import com.treode.store.{Bytes, StoreConfig}

trait SimpleMedic {

  def put (gen: Long, key: Bytes, value: Bytes)

  def delete (gen: Long, key: Bytes)

  def checkpoint (meta: SimpleTable.Meta)

  def close () (implicit launcher: Launch): SimpleTable
}

object SimpleMedic {

  def apply (id: TypeId) (
      implicit scheduler: Scheduler, recovery: Recovery, config: StoreConfig): SimpleMedic =
    new SynthMedic (id)
}
