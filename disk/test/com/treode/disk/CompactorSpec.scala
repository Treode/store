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

import java.util.ArrayList

import com.treode.async.{Async, Callback}, Async.async
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.disk.stubs.StubDiskEvents
import com.treode.notify.Notification
import com.treode.pickle.Picklers
import org.scalatest.FlatSpec

import DiskTestTools._

class CompactorSpec extends FlatSpec {

  implicit val config = DiskTestConfig()
  implicit val events = new StubDiskEvents
  val geom = DriveGeometry (8, 6, 1 << 18)
  val desc = PageDescriptor (0x27, Picklers.unit)

  class CompactionCaptor (implicit scheduler: StubScheduler) {

    private var next = Option.empty [(Compaction, Callback [Unit])]

    /** Start the compaction; records it for finish. */
    def compact (c: Compaction): Async [Unit] =
      async { cb =>
        assert (next.isEmpty)
        next = Some ((c, cb))
      }

    /** Check that the next compaction is for the given object, and then finish it. */
    def finish (id: ObjectId) {
      scheduler.run()
      assert (next.isDefined)
      val (c, cb) = next.get
      next = Option.empty
      assert (c.obj == id)
      scheduler.pass (cb, ())
      scheduler.run()
    }

    def register (launch: DiskLaunch): Unit =
      launch.compact (desc) (compact (_))
  }

  private def setup () (implicit scheduler: StubScheduler): (DiskAgent, CompactionCaptor) = {
    implicit val files = new StubFileSystem
    implicit val recovery = new RecoveryAgent
    files.create ("d1", 0, geom.blockBits)
    val launch = recovery.reattach().expectPass()
    val captor = new CompactionCaptor
    captor.register (launch)
    launch.launch()
    val agent = launch.controller.asInstanceOf [DiskAgent]
    agent.attach (geom, "d1") .expectPass (Notification.unit)
    (agent, captor)
  }

  "The Compactor" should "collapse requests, and run them one at a time" in {
    implicit val scheduler = StubScheduler.random()
    val (agent, captor) = setup()
    agent.compact (desc, 1)
    agent.compact (desc, 2)
    agent.compact (desc, 3)
    agent.compact (desc, 2)
    captor.finish (1)
    captor.finish (2)
    captor.finish (3)
  }}
