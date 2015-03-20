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

import scala.util.Random

import com.treode.async.{Async, Scheduler}, Async.{async, supply}
import com.treode.async.implicits._
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.StubScheduler
import com.treode.disk.{DiskLaunch, DiskRecovery}
import com.treode.disk.stubs.edit.{StubDisk, StubDiskChecks}
import com.treode.store.{Bytes, StoreConfig, StoreTestConfig, TableId}
import com.treode.tags.{Intensive, Periodic}
import org.scalatest.FreeSpec

import TierTestTools._

class TierSystemSpec extends FreeSpec with StubDiskChecks {

  private class TableTracker (id: TableId, nkeys: Int) (implicit config: StoreConfig)
  extends Tracker {

    type Medic = TestMedic
    type Struct = TestTable

    private var attempted = Map.empty [Int, Option [Int]] .withDefaultValue (None)
    private var accepted = Map.empty [Int, Option [Int]] .withDefaultValue (None)

    def recover () (implicit random: Random, scheduler: Scheduler, recovery: DiskRecovery): Medic =
      new TestMedic (id)

    def launch (medic: Medic) (implicit launch: DiskLaunch): Async [Struct] =
      medic.launch (launch)

    def put (table: TestTable, key: Int, value: Int): Async [Unit] = {
      attempted += key -> Some (value)
      for {
        _ <- table.put (key, value)
      } yield {
        accepted += key -> Some (value)
      }}

    def batch (table: TestTable, kvs: (Int, Int)*): Async [Unit] = {
      for ((key, value) <- kvs.latch)
        put (table, key, value)
    }

    def batches (
      table: TestTable,
      nbatches: Int,
      nputs: Int
    ) (implicit
      random: Random,
      scheduler: Scheduler
    ): Async [Unit] =
      for (_ <- (0 until nbatches).async)
        batch (table, random.nextPut (nkeys, nputs): _*)

    def verify (crashed: Boolean, table: Struct) (implicit scheduler: Scheduler): Async [Unit] =
      for {
        recovered <- table.toMap
      } yield {
        for (k <- accepted.keySet)
          assert (
            recovered.contains (k) || attempted (k) == None,
            s"Expected $k to be recovered")
        for ((k, v) <- recovered) {
          val expected = attempted (k) .toSet ++ accepted (k) .toSet
          assert (expected contains v,
              s"Expected $k to be ${expected mkString " or "}, found $v")
        }}

    override def toString = s"new TableTracker (${id.id}, $nkeys)"
  }

  private class TablePhase (nbatches: Int, nputs: Int) extends Effect [TableTracker] {

    def start (
      tracker: TableTracker,
      table: TestTable
    ) (implicit
      random: Random,
      scheduler: Scheduler,
      disk: StubDisk
    ): Async [Unit] =
      tracker.batches (table, nbatches, nputs)

    override def toString = s"new TablePhase ($nbatches, $nputs)"
  }

  "The TierTable should" - {

    for { (name, (nkeys, nputs, nbatches)) <- Seq (
        "recover with lots of writes"     -> (10000, 3, 100),
        "recover with lots of overwrites" -> (30, 3, 100))
    } name taggedAs (Intensive, Periodic) in {
      implicit val config = StoreTestConfig.storeConfig()
      manyScenarios (new TableTracker (0xC8, nkeys), new TablePhase (nbatches, nputs))
    }}}
