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
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.disk.{DiskController, DiskLaunch, DiskTestConfig, DriveAttachment, DriveChange,
  DriveGeometry, StubFileSystem}
import com.treode.disk.stubs.StubDiskEvents
import com.treode.notify.Notification
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

  /** Convenient methods for testing. */
  implicit class RichDiskController (controller: DiskController) {

    // TODO: Move this into the DiskController trait.
    def attach (attaches: DriveAttachment*): Async [Notification] =
      controller.asInstanceOf [DiskAgent] .change (DriveChange (attaches, Seq.empty))

    // TODO: Move this into the DiskController trait.
    def attach (geom: DriveGeometry, paths: Path*): Async [Notification] =
      attach (paths map (DriveAttachment (_, geom)): _*)

    // NOT TODO: These is for testing only.
    def attach (paths: String*) (implicit geom: DriveGeometry): Async [Notification] =
      attach (geom, paths map (Paths.get (_)): _*)

    def drain (paths: Path*): Async [Notification] =
      controller.asInstanceOf [DiskAgent] .change (DriveChange (Seq.empty, paths))

    def drain (paths: String*) (implicit geom: DriveGeometry): Async [Notification] =
      drain (paths map (Paths.get (_)): _*)

    def change (attach: Seq [String], drain: Seq [String]) (
          implicit geom: DriveGeometry): Async [Notification] = {
      val attachPaths = (attach map (Paths.get(_))) map (DriveAttachment (_, geom))
      val drainPaths = (drain map (Paths.get(_)))
      controller.asInstanceOf [DiskAgent] .change (DriveChange (attachPaths, drainPaths))
    }
  }

  def setup (paths: String*) (implicit scheduler: StubScheduler): RichDiskController = {
    implicit val files = new StubFileSystem
    implicit val recovery = new RecoveryAgent
    files.create(paths map (Paths.get (_)), 0, 1 << 14)
    val launch = recovery.reattach().expectPass()
    launch.launch()
    launch.controller
  }

  implicit val config = DiskTestConfig()
  implicit val events = new StubDiskEvents
  implicit val geom = DriveGeometry (8, 6, 1 << 14)

  "DriveGroup.change" should "reject nonexistant files" in {
    implicit val scheduler = StubScheduler.random()
    val controller = setup("f1", "f2", "f3")
    val e = controller.attach ("a") .expectFail [IllegalArgumentException]
    assertResult ("requirement failed: File a does not exist.") (e.getMessage)
  }

  it should "reject duplicate filenames (same operation)" in {
    implicit val scheduler = StubScheduler.random()
    val controller = setup("f1", "f2", "f3")
    val e = controller.attach ("f1", "f1").expectPass()
    scheduler.run()
    assert (e.list(0).en == "Already attaching: \"f1\"")
  }

  it should "reject duplicate filenames (different operation)" in {
    implicit val scheduler = StubScheduler.random()
    val controller = setup("f1", "f2", "f3")
    val e1 = controller.attach ("f1").expectPass()
    scheduler.run()
    val e2 = controller.attach ("f1").expectPass()
    scheduler.run()
    assert (e1.list.length == 0)
    assert (e2.list(0).en == "Already attached: \"f1\"")
  }

  it should "reject duplicate draining of the same file" in {
    implicit val scheduler = StubScheduler.random()
    val controller = setup("f1", "f2", "f3")
    val e1 = controller.attach ("f1").expectPass()
    val e2 = controller.drain ("f1", "f1").expectPass()
    scheduler.run()
    assert (e1.list.length == 0)
    assert (e2.list(0).en == "Already draining: \"f1\"")
  }

  it should "reject draining unattached files" in {
    implicit val scheduler = StubScheduler.random()
    val controller = setup("f1", "f2", "f3")
    val e1 = controller.drain ("f1").expectPass()
    scheduler.run()
    assert (e1.list(0).en == "Not attached: \"f1\"")
  }

  it should "reject attaching and draining a file in the same operation" in {
    implicit val scheduler = StubScheduler.random()
    val controller = setup("f1", "f2", "f3")
    val e1 = controller.change (Seq ("f1"), Seq ("f1")).expectPass()
    scheduler.run()
    assert (e1.list(0).en == "Not attached: \"f1\"")
  }

  "The DriveGroup" should "recover attached disks" taggedAs (Periodic) in {
    manyScenarios (new DriveGroupTracker, DriveGroupPhase)
  }}
