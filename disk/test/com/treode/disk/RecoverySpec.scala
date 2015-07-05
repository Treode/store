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

package com.treode.disk

import java.nio.file.Path

import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.disk.exceptions.ReattachException
import com.treode.disk.stubs.StubDiskEvents
import org.scalatest.FlatSpec

import DiskTestTools._

class RecoverySpec extends FlatSpec {

  implicit val config = DiskTestConfig()

  implicit val geom = DriveGeometry (8, 6, 1 << 14)

  class TrackReattachEvents extends StubDiskEvents {

    private var invoked = false
    private var reattached = Set.empty [Path]
    private var detached = Set.empty [Path]

    def expectReattaching (reattached: Path*) (detached: Path*) {
      assert (invoked)
      assertResult (reattached.toSet) (this.reattached)
      assertResult (detached.toSet) (this.detached)
    }

    def expectNotReattaching() {
      assert (!invoked)
    }

    override def reattachingDisks (reattached: Set [Path], detached: Set [Path]) {
      assert (!invoked)
      invoked = true
      this.reattached ++= reattached
      this.detached ++= detached
    }}

  /** Create the files, and then initialize the disk system with them. */
  def init (paths: Path*) (implicit files: StubFileSystem, geom: DriveGeometry) {
    implicit val scheduler = StubScheduler.random()
    implicit val events = new StubDiskEvents
    val recovery = new RecoveryAgent
    val launch = recovery.init (SystemId.zero) .expectPass()
    launch.launch()
    for (p <- paths)
      files.create (p, 0, geom.blockBits)
    launch.controller.attach (geom, paths: _*) .expectPass()
    scheduler.run()
  }

  /** Reopen the disk system with the files, and then drain them. */
  def drain (paths: Path*) (implicit files: StubFileSystem) {
    implicit val scheduler = StubScheduler.random()
    implicit val events = new StubDiskEvents
    val recovery = new RecoveryAgent
    val launch = recovery.reattach (paths: _*) .expectPass()
    launch.launch()
    launch.controller.drain (paths: _*) .expectPass()
    scheduler.run()
  }

  "The RecoveryAgent" should "not require a file that has been drained" in {
    implicit val files = new StubFileSystem
    init ("f1", "f2")
    drain ("f2")
    files.remove ("f2")
    implicit val scheduler = StubScheduler.random()
    implicit val events = new TrackReattachEvents
    val recovery = new RecoveryAgent
    recovery.reattach ("f1") .expectPass()
    events.expectReattaching ("f1") ()
  }

  it should "handle a path to reattach that's missing" in {
    implicit val files = new StubFileSystem
    init ("f1")
    implicit val scheduler = StubScheduler.random()
    implicit val events = new TrackReattachEvents
    val recovery = new RecoveryAgent
    val thrown = recovery.reattach ("f1", "f2") .expectFail [ReattachException]
    assertResult ("""File "f2" does not exist.""") (thrown.getMessage)
    events.expectNotReattaching()
  }

  it should "handle a path from the superblock that's missing" in {
    implicit val files = new StubFileSystem
    init ("f1", "f2")
    files.remove ("f2")
    implicit val scheduler = StubScheduler.random()
    implicit val events = new TrackReattachEvents
    val recovery = new RecoveryAgent
    val thrown = recovery.reattach ("f1") .expectFail [ReattachException]
    assertResult ("""File "f2" does not exist.""") (thrown.getMessage)
    events.expectNotReattaching()
  }

  it should "handle a path to reattach that has been detached" in {
    implicit val files = new StubFileSystem
    init ("f1", "f2")
    drain ("f1")
    implicit val scheduler = StubScheduler.random()
    implicit val events = new TrackReattachEvents
    val recovery = new RecoveryAgent
    recovery.reattach ("f1") .expectPass()
    events.expectReattaching ("f2") ("f1")
  }}
