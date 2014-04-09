package com.treode.store.tier

import com.treode.async.{Async, AsyncIterator}
import com.treode.disk.{Disks, RecordDescriptor}
import com.treode.store.StorePicklers

private trait TestTable {

  def get (key: Int): Async [Option [Int]]

  def iterator: AsyncIterator [TestCell]

  def put (key: Int, value: Int): Async [Unit]

  def delete (key: Int): Async [Unit]
}

private object TestTable {

  trait Recovery {
    def launch (implicit launch: Disks.Launch): Async [TestTable]
  }

  val descriptor = {
    import StorePicklers._
    TierDescriptor (0x28)
  }

  val put = {
    import StorePicklers._
    RecordDescriptor (0x09, tuple (ulong, int, int))
  }

  val delete = {
    import StorePicklers._
    RecordDescriptor (0x37, tuple (ulong, int))
  }

  val checkpoint = {
    import StorePicklers._
    RecordDescriptor (0xD1, tierMeta)
  }}
