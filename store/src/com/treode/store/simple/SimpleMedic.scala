package com.treode.store.simple

import com.treode.disk.Disks
import com.treode.store.{Bytes, StoreConfig}

trait SimpleMedic {

  def put (gen: Long, key: Bytes, value: Bytes)

  def delete (gen: Long, key: Bytes)

  def close(): SimpleTable
}

object SimpleMedic {

  def apply () (implicit disks: Disks, config: StoreConfig): SimpleMedic =
    new SynthMedic (config)
}
