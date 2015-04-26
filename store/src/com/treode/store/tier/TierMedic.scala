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

import com.treode.async.Scheduler
import com.treode.disk.DiskLaunch
import com.treode.store.{Bytes, Cell, StoreConfig, TableId, TxClock}

private [store] trait TierMedic {

  def put (gen: Long, key: Bytes, time: TxClock, value: Bytes)

  def delete (gen: Long, key: Bytes, time: TxClock)

  def receive (gen: Long, novel: Seq [Cell])

  def compact (meta: TierTable.Compaction)

  def checkpoint (meta: TierTable.Checkpoint)

  def close () (implicit launch: DiskLaunch): TierTable
}

private [store] object TierMedic {

  def apply (
      desc: TierDescriptor,
      id: TableId
  ) (implicit
      scheduler: Scheduler,
      config: StoreConfig
  ): TierMedic =
    new SynthMedic (desc, id)
}
