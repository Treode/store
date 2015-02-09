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

package com.treode.store.atomic

import scala.collection.SortedMap
import scala.util.Random

import com.treode.async.Async, Async.supply
import com.treode.async.implicits._
import com.treode.async.stubs.StubScheduler
import com.treode.cluster.HostId
import com.treode.cluster.stubs.{StubCluster, StubHost, StubNetwork}
import com.treode.disk.stubs.{StubDisk, StubDiskDrive}
import com.treode.store._
import com.treode.store.catalog.Catalogs
import org.scalatest.FreeSpec

import AtomicTestTools._
import Fruits.AllFruits
import StubScanDeputy.fruitsId

private class StubScanDeputy (
    val localId: HostId
) (implicit
    val random: Random,
    val scheduler: StubScheduler,
    val cluster: StubCluster
) extends StubHost {

  type StubTable = SortedMap [Key, Option [Bytes]]

  val fruitsCells: Seq [Cell] =
    for ((k, i) <- Fruits.AllFruits.zipWithIndex)
      yield Cell (k, 0, Some (Bytes (i)))

  val fruitsMap: StubTable =
    SortedMap ((for (c <- fruitsCells) yield c.timedKey -> c.value): _*)

  val tables: Map [TableId, StubTable] =
    Map (fruitsId -> fruitsMap) .withDefaultValue (SortedMap.empty)

  ScanDeputy.scan.listen { case ((table, start, window, slice, batch), from) =>
    tables (table)
    .from (start.bound)
    .filter (start <* _._1)
    .map {case (key, value) => Cell (key.key, key.time, value)}
    .batch
    .rebatch (Batch (random.nextInt (9), Int.MaxValue))
    .map {case (cells, last) => (cells, last.isEmpty)}
  }

  cluster.startup()
}

private object StubScanDeputy {

  val fruitsId = TableId (0x11)

  def install () (implicit
      random: Random,
      scheduler: StubScheduler,
      network: StubNetwork
  ): Async [StubHost] = {

    val id = HostId (random.nextLong)
    val drive = new StubDiskDrive

    implicit val config = StoreTestConfig()
    import config._

    implicit val cluster = new StubCluster (id)
    implicit val recovery = StubDisk.recover()
    implicit val library = new Library
    implicit val _catalogs = Catalogs.recover()

    for {
      launch <- recovery.attach (drive)
      catalogs <- _catalogs.launch (launch, cluster)
    } yield {
      launch.launch()
      new StubScanDeputy (id) (random, scheduler, cluster)
    }}}
