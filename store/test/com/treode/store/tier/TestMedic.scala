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
import com.treode.disk.{DiskLaunch, DiskRecovery}
import com.treode.store.{Bytes, StoreConfig, TableId, TxClock}

import Async.supply
import TestTable.{checkpoint, compact, delete, descriptor, put}

/** Wrap the production `SynthMedic` with something that's easier to handle in testing. */
private class TestMedic (
    id: TableId
) (implicit
    scheduler: StubScheduler,
    recovery: DiskRecovery,
    config: StoreConfig
) extends TestTable.Medic {

  val medic = new SynthMedic (descriptor, id.id)

  put.replay { case (gen, key, value) =>
    medic.put (gen, Bytes (key), TxClock.MinValue, Bytes (value))
  }

  delete.replay { case (gen, key) =>
    medic.delete (gen, Bytes (key), TxClock.MinValue)
  }

  compact.replay { meta =>
    medic.compact (meta)
  }

  checkpoint.replay { meta =>
    medic.checkpoint (meta)
  }

  def launch (implicit launch: DiskLaunch): Async [TestTable] = supply {
    import launch.{checkpoint, disk}
    val table = new TestTable (medic.close())
    checkpoint (table.checkpoint())
    descriptor.handle (table)
    table
  }}
