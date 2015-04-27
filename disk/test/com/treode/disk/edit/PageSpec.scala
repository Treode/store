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

import java.nio.file.{Path, Paths}
import scala.util.Random

import com.treode.async.{Async, Scheduler}, Async.supply
import com.treode.async.implicits._
import com.treode.disk.{Disk, DiskConfig, DiskLaunch, DiskTestConfig, DriveGeometry, ObjectId,
  PageDescriptor, Position, Stuff}
import com.treode.disk.stubs.Counter
import com.treode.pickle.Picklers
import com.treode.tags.Periodic
import org.scalatest.FreeSpec

class PageSpec extends FreeSpec with DiskChecks {

  val desc = Stuff.pager

  private class PageTracker (nobjects: Int, npages: Int, nitems: Int) extends Tracker {

    /** Hook Stuff into recovery and compaction. */
    class UserObject (id: ObjectId, private var page: Int) {

      private var written = Map.empty [Long, List [(Long, Int, Position)]]
      private var released = Set.empty [Long]
      private var gen = 0L

      def write () (implicit random: Random, agent: DiskAgent): Async [Unit] = {
        while (page == 0) {
          gen += 1
          page = random.nextInt (npages + 1)
        }
        val seed = random.nextLong()
        val length = random.nextInt (nitems)
        for {
          pos <- agent.write (desc, id, gen, Stuff (seed, length))
        } yield {
          written += gen -> ((seed, length, pos) :: written.getOrElse (gen, List.empty))
        }}

      def claim () (implicit launch: DiskLaunch): Unit =
        launch.claim (desc, id, written.keySet -- released)

      def compact (gens: Set [Long]) (implicit disk: Disk): Async [Unit] =
        supply {
          released ++= gens
          disk.release (desc, id, gens)
        }

      def verify () (implicit scheduler: Scheduler, agent: DiskAgent): Async [Unit] =
        for {
          (gen, pages) <- (written -- released).async
          (seed, length, pos) <- pages.async
        } {
          val expected = Stuff (seed, length)
          for (actual <- agent.read (desc, pos)) yield
            assert (expected == actual)
        }}

    /** Track the objects we've written during the test. */
    private var objects = Map.empty [ObjectId, UserObject]

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
      getObject (random.nextInt (nobjects))

    def recover () (implicit
      scheduler: Scheduler,
      recovery: RecoveryAgent
    ) {
      // noop
    }

    def launch () (implicit
      random: Random,
      scheduler: Scheduler,
      launch: DiskLaunch
    ) {
      import launch.disk
      for (obj <- objects.values)
        obj.claim()
      launch.compact (desc) { compaction =>
        getObject (compaction.obj) compact (compaction.gens)
      }}

    /** Write a new objects. */
    def write () (implicit
      random: Random,
      scheduler: Scheduler,
      agent: DiskAgent
    ): Async [Unit] =
      randomObject().write()

    /** Write nbatches of nwrites new objects. */
    def write (
      nbatches: Int,
      nwrites: Int
    ) (implicit
      random: Random,
      scheduler: Scheduler,
      agent: DiskAgent
    ): Async [Unit] = {
      var i = 0
      scheduler.whilst (i < nbatches) {
        i += 1
        Async.count (nwrites) (write())
      }}

    def verify (
      crashed: Boolean
    ) (implicit
      scheduler: Scheduler,
      agent: DiskAgent
    ): Async [Unit] =
      for (obj <- objects.values.async)
        obj.verify()

    override def toString = s"new PageTracker ($nobjects, $npages, $nitems)"
  }

  private case class PageBatch (nbatches: Int, nwrites: Int) extends Effect [PageTracker] {

    def start (
      tracker: PageTracker
    ) (implicit
      random: Random,
      scheduler: Scheduler,
      agent: DiskAgent,
      drives: DrivesTracker
    ): Async [Unit] =
      tracker.write (nbatches, nwrites)

    override def toString = s"PageBatch ($nbatches, $nwrites)"
  }

  "The Pager should read what was written" - {

    implicit val config = DiskTestConfig (maximumPageBytes = 1 << 14)
    implicit val geom = DriveGeometry (14, 7, 1 << 20)

    for {
      nbatches <- Seq (0, 1, 2, 3)
      nwrites <- Seq (0, 1, 2, 3)
      if (nbatches != 0 && nwrites != 0 || nbatches == nwrites)
    } s"for $nbatches batches of $nwrites writes" taggedAs (Periodic) in {
      manyScenarios (new PageTracker (3, 7, 7), PageBatch (nbatches, nwrites))
    }

    for {
      (nbatches, nwrites, nobjects) <- Seq ((7, 7, 7), (16, 16, 20))
    } s"for $nbatches batches of $nwrites writes" taggedAs (Periodic) in {
      manyScenarios (new PageTracker (nobjects, 7, 7), PageBatch (nbatches, nwrites))
    }

    for {
      (nbatches, nwrites, nobjects) <- Seq ((20, 100, 10))
    } s"for $nbatches batches of $nwrites writes" taggedAs (Periodic) in {
      someScenarios (new PageTracker (nobjects, 7, 7), PageBatch (nbatches, nwrites))
    }}}
