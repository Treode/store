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

package com.treode.store.catalog

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.{Cluster, ReplyTracker}
import com.treode.disk.Disk
import com.treode.store._

private class CatalogKit (val broker: Broker) (implicit
    val random: Random,
    val scheduler: Scheduler,
    val cluster: Cluster,
    val disk: Disk,
    val library: Library,
    val config: StoreConfig
) extends Catalogs {

  val acceptors = new Acceptors (this)

  val proposers = new Proposers (this)

  def propose (key: CatalogId, patch: Patch): Async [Patch] =
    proposers.propose (random.nextInt (17) + 1, key, patch)

  def listen [C] (desc: CatalogDescriptor [C]) (handler: C => Any): Unit =
    broker.listen (desc) (handler)

  def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Async [Unit] = {
    for {
      patch <- broker.diff (desc) (version, cat)
      chosen <- propose (desc.id, patch)
      _ <- broker.patch (desc.id, chosen)
    } yield {
      if (patch.checksum != chosen.checksum)
        throw new StaleException
    }}}
