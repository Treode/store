/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treode.store.tier

import com.treode.async.Async
import com.treode.async.stubs.StubScheduler
import com.treode.disk.Disk
import com.treode.store.{Bytes, Store, TableId, TxClock}

import Async.supply
import TestTable.{checkpoint, delete, descriptor, put}

private class TestMedic (
    id: TableId
) (implicit
    scheduler: StubScheduler,
    recovery: Disk.Recovery,
    config: Store.Config
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
    import launch.{checkpoint, disk}
    val table = new TestTable (medic.close())
    checkpoint (table.checkpoint())
    descriptor.handle (table)
    table
  }}
