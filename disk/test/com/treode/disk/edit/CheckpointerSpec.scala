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

import com.treode.async.Async, Async.supply
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.disk.{DiskConfig, DiskController, DiskTestTools, DriveGeometry, StubFileSystem},
  DiskTestTools._
import com.treode.disk.stubs.StubDiskEvents
import com.treode.notify.Notification
import org.scalatest.FlatSpec

class CheckpointerSpec extends FlatSpec {

  implicit val config = DiskConfig.suggested
  implicit val events = new StubDiskEvents
  val geom = DriveGeometry (8, 6, 1 << 18)

  private def setup (f: => Unit) (implicit scheduler: StubScheduler): DiskAgent = {
    implicit val files = new StubFileSystem

    implicit val recovery = new RecoveryAgent
    files.create ("d1", 0, 1 << 14)
    val launch = recovery.reattach().expectPass()
    launch.checkpoint (supply (f))
    launch.launch()
    val agent = launch.controller.asInstanceOf [DiskAgent]
    agent.attach ("d1", geom) .expectPass (Notification.empty)
    agent
  }

  "The Checkpointer" should "invoke registered checkpointers" in {
    implicit val scheduler = StubScheduler.random()
    var invoked = 0
    val controller = setup (invoked += 1)
    controller.checkpoint().expectPass()
    assert (invoked == 1)
  }

  it should "queue and collapse requests" in {
    implicit val scheduler = StubScheduler.random()
    var invoked = 0
    val controller = setup (invoked += 1)
    // First request runs.
    val cb1 = controller.checkpoint().capture()
    // Second request is queued.
    val cb2 = controller.checkpoint().capture()
    // Third request is queued and run with second.
    controller.checkpoint().expectPass()
    cb1.assertInvoked()
    cb2.assertInvoked()
    assert (invoked == 2)
  }}
