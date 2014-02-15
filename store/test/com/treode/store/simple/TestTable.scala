package com.treode.store.simple

import com.treode.async.{Callback, Scheduler, callback}
import com.treode.disk.{Disks, RecordDescriptor, Recovery, RootDescriptor}
import com.treode.store.{Bytes, StoreConfig, StorePicklers}

private trait TestTable {

  def get (key: Int, cb: Callback [Option [Int]])

  def iterator (cb: Callback [TestIterator])

  def put (key: Int, value: Int, cb: Callback [Unit])

  def delete (key: Int, cb: Callback [Unit])
}

private object TestTable {

  val pager = TierPage.pager (0x70121099)

  val root = {
    import StorePicklers._
    RootDescriptor (0x2B30D8AF, simpleMeta)
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
    RecordDescriptor (0xA67C3DD1, simpleMeta)
  }

  def recover (cb: Callback [TestTable]) (
      implicit scheduler: Scheduler, recovery: Recovery, config: StoreConfig) {

    val medic = SimpleMedic (0x691FC3F3)

    root.reload { tiers => implicit reload =>
      medic.checkpoint (tiers)
      reload.ready()
    }

    put.replay { case (gen, key, value) =>
      medic.put (gen, Bytes (key), Bytes (value))
    }

    delete.replay { case (gen, key) =>
      medic.delete (gen, Bytes (key))
    }

    recovery.launch { implicit launch =>
      import launch.disks
      val table = medic.close()
      root.checkpoint (table.checkpoint _)
      //pager.handle (table)
      cb (new LoggedTable (table))
      launch.ready()
    }}
}
