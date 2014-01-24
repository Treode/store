package com.treode.store.simple

import com.treode.async.Callback
import com.treode.disk.{Disks, Position}
import com.treode.store.{Bytes, SimpleCell, StoreConfig, StorePicklers}

trait SimpleTable {

  def get (key: Bytes, cb: Callback [Option [Bytes]])

  def iterator (cb: Callback [SimpleIterator])

  def put (key: Bytes, value: Bytes): Long

  def delete (key: Bytes): Long

  def checkpoint (cb: Callback [SimpleTable.Meta])
}

object SimpleTable {

  case class Meta (gen: Long, tiers: Array [Position])

  object Meta {

    val pickle = {
      import StorePicklers._
      wrap (long, array (pos))
      .build ((Meta.apply _).tupled)
      .inspect (v => (v.gen, v.tiers))
    }}

  trait Medic {

    def put (gen: Long, key: Bytes, value: Bytes)

    def delete (gen: Long, key: Bytes)
  }

  def recover (meta: Meta, config: StoreConfig) (implicit disks: Disks): Medic =
    new SynthMedic (meta, config)
}
