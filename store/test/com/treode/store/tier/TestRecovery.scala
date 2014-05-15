package com.treode.store.tier

import com.treode.async.Async
import com.treode.async.stubs.StubScheduler
import com.treode.disk.Disks
import com.treode.store.{Bytes, StoreConfig, TableId, TxClock}

import Async.supply
import TestTable.{checkpoint, delete, descriptor, put}

private class TestRecovery (
    id: TableId
) (implicit
    scheduler: StubScheduler,
    recovery: Disks.Recovery,
    config: StoreConfig
) extends TestTable.Recovery {

  val medic = TierMedic (descriptor, id.id)

  checkpoint.replay { meta =>
    medic.checkpoint (meta)
  }

  put.replay { case (gen, key, value) =>
    medic.put (gen, Bytes (key), TxClock.zero, Bytes (value))
  }

  delete.replay { case (gen, key) =>
    medic.delete (gen, Bytes (key), TxClock.zero)
  }

  def launch (implicit launch: Disks.Launch): Async [TestTable] = supply {
    import launch.{checkpoint, disks}
    val table = new LoggedTable (medic.close())
    checkpoint (table.checkpoint())
    descriptor.handle (table)
    table
  }}
