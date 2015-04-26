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
import com.treode.disk.{Compaction, DiskLaunch, ObjectId, PageDescriptor, PageHandler, TypeId}
import com.treode.store.{Cell, Residents, StorePicklers, TableId}
import com.treode.pickle.Pickler

private [store] class TierDescriptor private (
    val id: TypeId
) (
    val residency: (Residents, TableId, Cell) => Boolean
) {

  private [tier] val pager = PageDescriptor (id, TierPage.pickler)

  def claim (obj: ObjectId, gens: Set [Long]) (implicit launch: DiskLaunch): Unit =
    launch.claim (pager, obj, gens)

  def compact (f: Compaction => Async [Unit]) (implicit launch: DiskLaunch): Unit =
    launch.compact (pager) (f)

  def handle (handler: PageHandler) (implicit launch: DiskLaunch): Unit =
    pager.handle (handler)

  override def toString = s"TierDescriptor($id)"
}

private [store] object TierDescriptor {

  def apply (id: TypeId) (residency: (Residents, TableId, Cell) => Boolean): TierDescriptor =
    new TierDescriptor (id) (residency)
}
