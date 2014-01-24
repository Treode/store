package com.treode.store.simple

import com.treode.async.Callback
import com.treode.disk.Disks
import com.treode.store.{Bytes, SimpleCell, StoreConfig}

trait SimpleTable {

  def get (key: Bytes, cb: Callback [Option [Bytes]])

  def iterator (cb: Callback [SimpleIterator])

  def delete (key: Bytes): Long

  def put (key: Bytes, value: Bytes): Long

  def checkpoint (cb: Callback [Unit])

  def put (gen: Long, key: Bytes, value: Bytes)

  def deleted (gen: Long, key: Bytes)
}

object SimpleTable {

  def apply (config: StoreConfig) (implicit disks: Disks): SimpleTable =
    new SynthTable (config)
}
