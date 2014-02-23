package com.treode.store.tier

import com.treode.async.{Async, AsyncIterator, Callback, Scheduler}
import com.treode.disk.{Disks, RecordDescriptor, RootDescriptor}
import com.treode.store.{Bytes, StoreConfig, StorePicklers}

import Async.{async, supply}

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
  }

  def recover() (implicit scheduler: Scheduler, recovery: Disks.Recovery, config: StoreConfig): Recovery = {

      val medic = TierMedic (descriptor)

      root.reload { tiers => implicit reload =>
        medic.checkpoint (tiers)
        supply(())
      }

      put.replay { case (gen, key, value) =>
        medic.put (gen, Bytes (key), Bytes (value))
      }

      delete.replay { case (gen, key) =>
        medic.delete (gen, Bytes (key))
      }

      new Recovery {

        def launch (implicit launch: Disks.Launch): Async [TestTable] = {
          import launch.disks
          val table = medic.close()
          root.checkpoint (table.checkpoint())
          //pager.handle (table)
          supply (new LoggedTable (table))
        }}}}
