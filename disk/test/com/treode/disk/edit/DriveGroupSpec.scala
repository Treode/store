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
import com.treode.disk.{DiskLaunch, DiskTestConfig, DriveGeometry}
import com.treode.pickle.Picklers
import com.treode.tags.Periodic
import org.scalatest.FlatSpec

class DriveGroupSpec extends FlatSpec with DiskChecks {

  /** The DriveGroup testing strategy is a no-op because the DrivesTracker in DiskChecks
    * already verifies everything we need. This no-op strategy assures us that the DriveGroup
    * works sans interference from other components.
    */
  private class DriveGroupTracker extends Tracker {

    def recover () (implicit
      scheduler: Scheduler,
      recovery: RecoveryAgent
    ) {
      // no-op
    }

    def launch () (implicit
      random: Random,
      scheduler: Scheduler,
      launch: DiskLaunch
    ) {
      // no-op
    }

    def verify (
      crashed: Boolean
    ) (implicit
      scheduler: Scheduler,
      agent: DiskAgent
    ): Async [Unit] =
      supply {
        // no-op
      }

    override def toString = "new DriveGroupTracker"
  }

  /** The DriveGroup effect is a no-op because the scenarios in DiskChecks already verify
    * everything we need. This no-op effect assures us that the DriveGroup works sans interference
    * from other components.
    */
  private case object DriveGroupPhase extends Effect [DriveGroupTracker] {

    def start (
      tracker: DriveGroupTracker
    ) (implicit
      random: Random,
      scheduler: Scheduler,
      agent: DiskAgent,
      drives: DrivesTracker
    ): Async [Unit] =
      supply {
        // no-op
      }

    override def toString = "DriveGroupPhase"
  }

  "The DriveGroup" should "recover attached disks" taggedAs (Periodic) in {
    implicit val config = DiskTestConfig()
    implicit val geom = DriveGeometry (8, 6, 1 << 14)
    manyScenarios (new DriveGroupTracker, DriveGroupPhase)
  }}
