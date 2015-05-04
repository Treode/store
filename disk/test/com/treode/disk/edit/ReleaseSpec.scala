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
import com.treode.disk.{Compaction, Disk, DiskConfig, DiskLaunch, DiskTestConfig, DriveGeometry,
  ObjectId, PageDescriptor, Position, Stuff}
import com.treode.disk.stubs.Counter
import com.treode.pickle.Picklers
import com.treode.tags.Periodic
import org.scalatest.FreeSpec

class ReleaseSpec extends FreeSpec with DiskChecks {

  val desc = Stuff.pager

  private class ReleaseTracker extends Tracker {

    def compact (compaction: Compaction) (implicit disk: Disk): Async [Unit] =
      supply {
        disk.release (desc, compaction.obj, compaction.gens)
      }

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
      launch.compact (desc) (compact (_))
    }

    def verify (
      crashed: Boolean
    ) (implicit
      scheduler: Scheduler,
      agent: DiskAgent
    ): Async [Unit] =
      supply {
        // noop
      }

    override def toString = s"new ReleaseTracker"
  }

  private case class ReleaseBatch (nbatches: Int, nwrites: Int, nitems: Int)
  extends Effect [ReleaseTracker] {

    /** An object released during an epoch should remain until leaving the epoch. */
    def write (tracker: ReleaseTracker) (implicit
      random: Random,
      scheduler: Scheduler,
      agent: DiskAgent
    ): Async [Unit] = {
      val id = ObjectId (random.nextLong())
      val seed = random.nextLong()
      val length = random.nextInt (nitems)
      val expected = Stuff (seed, length)
      agent.join {
        for {
          pos <- agent.write (desc, id, 0, expected)
          _ = agent.release (desc, id, Set (0))
          actual <- agent.read (desc, pos)
        } yield {
          assert (expected == actual)
        }}}

    def start (
      tracker: ReleaseTracker
    ) (implicit
      random: Random,
      scheduler: Scheduler,
      agent: DiskAgent,
      drives: DrivesTracker
    ): Async [Unit] = {
      var i = 0
      scheduler.whilst (i < nbatches) {
        i += 1
        Async.count (nwrites) (write (tracker))
      }}

    override def toString = s"ReleaseBatch ($nbatches, $nwrites, $nitems)"
  }

  "The Releaser should hold items released during an epoch until the end of the epoch" - {

    implicit val config = DiskTestConfig (maximumPageBytes = 1 << 14)
    implicit val geom = DriveGeometry (14, 7, 1 << 20)

    for {
      nbatches <- Seq (0, 1, 2, 3, 7)
      nwrites <- Seq (0, 1, 2, 3, 7)
      if (nbatches != 0 && nwrites != 0 || nbatches == nwrites)
    } s"for $nbatches batches of $nwrites writes" taggedAs (Periodic) in {
      onePhaseScenarios (new ReleaseTracker, ReleaseBatch (nbatches, nwrites, 7))
    }}}
