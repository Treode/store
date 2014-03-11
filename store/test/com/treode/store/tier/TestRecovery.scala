package com.treode.store.tier

import com.treode.async.{Async, Scheduler}
import com.treode.disk.Disks
import com.treode.store.{Bytes, StoreConfig, TableId}

import Async.supply
import TestTable.{delete, descriptor, put}

private class TestRecovery (
    id: TableId
) (implicit
    scheduler: Scheduler,
    recovery: Disks.Recovery,
    config: StoreConfig
) extends TestTable.Recovery {

  val medic = TierMedic (descriptor, id.id)

  put.replay { case (gen, key, value) =>
    medic.put (gen, Bytes (key), Bytes (value))
  }

  delete.replay { case (gen, key) =>
    medic.delete (gen, Bytes (key))
  }

  def launch (implicit launch: Disks.Launch): Async [TestTable] = {
    import launch.disks
    val table = medic.close()
    //pager.handle (table)
    supply (new LoggedTable (table))
  }}
