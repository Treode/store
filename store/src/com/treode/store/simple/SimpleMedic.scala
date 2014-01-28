package com.treode.store.simple

import com.treode.disk.{Disks, TypeId}
import com.treode.store.{Bytes, StoreConfig}

trait SimpleMedic {

  def put (gen: Long, key: Bytes, value: Bytes)

  def delete (gen: Long, key: Bytes)

  def close(): SimpleTable
}

object SimpleMedic {

  def apply (id: TypeId) (implicit disks: Disks, config: StoreConfig): SimpleMedic =
    new SynthMedic (id)
}
