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

package com.treode.disk.edit

import scala.util.Random

import com.treode.async.{Async, Scheduler}, Async.supply
import com.treode.disk.{Disk, DiskLaunch, DiskRecovery, ObjectId, TypeId}
import com.treode.disk.stubs.edit.{StubDisk, StubDiskChecks}
import org.scalatest.FreeSpec

class SegmentLedgerSpec extends FreeSpec with StubDiskChecks {

  private class SegmentLedgerTracker (nobjects: Int, npages: Int) extends Tracker {

    type Medic = SegmentLedgerMedic

    type Struct = SegmentLedger

    class UserObject (id: ObjectId, private var page: Int) {

      private val tracker = SegmentLedgerTracker.this
      private var freeable = Set.empty [Int]
      private var group = 0

      def alloc (ledger: SegmentLedger) (implicit random: Random): Async [Unit] = {
        while (page == 0) {
          freeable += group
          group += 1
          page = random.nextInt (npages + 1)
        }
        page -= 1
        tracker.alloc (ledger, id, group, random.nextInt (nbytes))
      }}

    private val disk = 0
    private val seg = 0
    private val typ: TypeId = 0
    private val nbytes = 100

    private var objects = Map.empty [ObjectId, UserObject]
    private var allocating = Map.empty [(ObjectId, Long), Int] .withDefaultValue (0)
    private var allocated = Map.empty [(ObjectId, Long), Int] .withDefaultValue (0)

    def recover () (implicit scheduler: Scheduler, recovery: DiskRecovery): Medic =
      new SegmentLedgerMedic (recovery)

    def launch (medic: Medic) (implicit scheduler: Scheduler, launch: DiskLaunch): Async [Struct] =
      medic.close() .map (_._1)

    def getObject (id: ObjectId) (implicit random: Random): UserObject =
      objects.get (id) match {
        case Some (obj) =>
          obj
        case None =>
          val obj = new UserObject (id, random.nextInt (npages))
          objects += id -> obj
          obj
      }

    def randomObject () (implicit random: Random): UserObject =
      getObject (ObjectId (random.nextInt (nobjects)))

    def alloc (ledger: SegmentLedger, obj: ObjectId, gen: Long, bytes: Int): Async [Unit] = {
      val id = (obj, gen)
      val tally = new PageTally
      tally.alloc (typ, obj, gen, bytes)
      allocating += id -> (allocating (id) + bytes)
      for {
        _ <- ledger.alloc (disk, seg, 0, tally)
      } yield {
        allocated += id -> (allocated (id) + bytes)
      }}

    def batches (
      nbatches: Int,
      nallocs: Int,
      ledger: SegmentLedger
    ) (implicit
      random: Random,
      scheduler: Scheduler
    ): Async [Unit] = {
      var i = 0
      scheduler.whilst (i < nbatches) {
        i += 1
        Async.count (nallocs) (randomObject().alloc (ledger))
      }}

    def verify (crashed: Boolean, ledger: Struct) (implicit scheduler: Scheduler): Async [Unit] =
      supply {
        val docket = ledger.docket
        for ((page, bytes) <- docket; id = (page.obj, page.gen))
          assert (allocated (id) <= bytes && bytes <= allocating (id))
        for ((id @ (obj, grp), bytes) <- allocated)
          assert (bytes <= docket (typ, obj, grp, disk, seg))
      }

    override def toString = s"new SegmentLedgerTracker ($nobjects, $npages)"
  }

  private class SegmentLedgerPhase (nbatches: Int, nallocs: Int)
  extends Effect [SegmentLedgerTracker] {

    def start (
      tracker: SegmentLedgerTracker,
      ledger: SegmentLedger
    ) (implicit
      random: Random,
      scheduler: Scheduler,
      disk: StubDisk
    ): Async [Unit] =
      tracker.batches (nbatches, nallocs, ledger)

    override def toString = s"new SegmentLedgerPhase ($nbatches, $nallocs)"
  }

  "The SegmentLedger should record and recover" - {

    /*"in" in {
      twoPhases (new SegmentLedgerTracker (1, 3), 0x6A43A56B21F58B4FL) ((new SegmentLedgerPhase (3, 1),2), (Checkpoint (1),7), (Drain,7)) ((new SegmentLedgerPhase (3, 1),2147483647))
    }*/

    for {
      nbatches <- Seq (0, 1, 2, 3)
      nallocs <- Seq (0, 1, 2, 3)
      if (nbatches != 0 && nallocs != 0 || nbatches == nallocs)
    } s"for $nbatches batches of $nallocs allocations" in {
      manyScenarios (new SegmentLedgerTracker (1, 3), new SegmentLedgerPhase (nbatches, nallocs))
    }

    for {
      (nbatches, nallocs, nobjects) <- Seq ((7, 7, 10), (20, 20, 40), (20, 100, 10))
    } s"for $nbatches batches of $nallocs allocations" in {
      manyScenarios (new SegmentLedgerTracker (nobjects, 7), new SegmentLedgerPhase (nbatches, nallocs))
    }}}
