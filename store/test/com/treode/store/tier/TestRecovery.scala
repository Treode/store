package com.treode.store.tier

import com.treode.async.{Async, Scheduler}
import com.treode.disk.Disks
import com.treode.store.{Bytes, StoreConfig, TableId}

import Async.supply
import TestTable.{checkpoint, delete, descriptor, put}

private class TestRecovery (
    id: TableId
) (implicit
    scheduler: Scheduler,
    recovery: Disks.Recovery,
    config: StoreConfig
) extends TestTable.Recovery {

  val medic = TierMedic (descriptor, id.id)

  checkpoint.replay { meta =>
    medic.checkpoint (meta)
  }

  put.replay { case (gen, key, value) =>
    medic.put (gen, Bytes (key), Bytes (value))
  }

  delete.replay { case (gen, key) =>
    medic.delete (gen, Bytes (key))
  }

  def launch (implicit launch: Disks.Launch): Async [TestTable] = supply {
    import launch.{checkpoint, disks}
    val table = new LoggedTable (medic.close())
    checkpoint (table.checkpoint())
    //pager.handle (table)
    table
  }}
