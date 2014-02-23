package com.treode.store.tier

import com.treode.async.{Async, AsyncIterator}
import com.treode.disk.{Disks, RecordDescriptor, RootDescriptor}
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
    TierDescriptor (0x0B918C28, string, int)
  }

  val root = {
    import StorePicklers._
    RootDescriptor (0x2B30D8AF, tierMeta)
  }

  val put = {
    import StorePicklers._
    RecordDescriptor (0x6AC99D09, tuple (ulong, int, int))
  }

  val delete = {
    import StorePicklers._
    RecordDescriptor (0x4D620837, tuple (ulong, int))
  }

  val checkpoint = {
    import StorePicklers._
    RecordDescriptor (0xA67C3DD1, tierMeta)
  }}
