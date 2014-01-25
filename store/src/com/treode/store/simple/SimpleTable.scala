package com.treode.store.simple

import java.util.concurrent.locks.ReentrantReadWriteLock

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

    def close(): SimpleTable
  }

  def apply () (implicit disks: Disks, config: StoreConfig): SimpleTable = {
    val lock = new ReentrantReadWriteLock
    new SynthTable (config, lock, 0, newMemTable, newMemTable, Array.empty)
  }

  def medic () (implicit disks: Disks, config: StoreConfig): Medic =
    new SynthMedic (config)
}
