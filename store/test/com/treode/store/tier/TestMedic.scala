package com.treode.store.tier

import com.treode.async.Async
import com.treode.async.stubs.StubScheduler
import com.treode.disk.Disk
import com.treode.store.{Bytes, StoreConfig, TableId, TxClock}

import Async.supply
import TestTable.{checkpoint, delete, descriptor, put}

private class TestMedic (
    id: TableId
) (implicit
    scheduler: StubScheduler,
    recovery: Disk.Recovery,
    config: StoreConfig
) extends TestTable.Medic {

  val medic = TierMedic (descriptor, id.id)

  checkpoint.replay { meta =>
    medic.checkpoint (meta)
  }

  put.replay { case (gen, key, value) =>
    medic.put (gen, Bytes (key), TxClock.MinValue, Bytes (value))
  }

  delete.replay { case (gen, key) =>
    medic.delete (gen, Bytes (key), TxClock.MinValue)
  }

  def launch (implicit launch: Disk.Launch): Async [TestTable] = supply {
    import launch.{checkpoint, disks}
    val table = new TestTable (medic.close())
    checkpoint (table.checkpoint())
    descriptor.handle (table)
    table
  }}
