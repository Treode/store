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

import com.treode.async.Async, Async.supply
import com.treode.async.stubs.{AsyncCaptor, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.disk.stubs.StubDiskEvents
import com.treode.notify.Notification
import com.treode.pickle.Picklers
import org.scalatest.FlatSpec

import DiskTestTools._

class CheckpointerSpec extends FlatSpec {

  implicit val config = DiskTestConfig (checkpointBytes = 128)
  implicit val events = new StubDiskEvents
  val geom = DriveGeometry (8, 6, 1 << 18)

  // We use arrays of longs for our sample log records.
  val desc = {
    import Picklers._
    RecordDescriptor (0x58, seq (fixedLong))
  }

  // 65 bytes long.
  val large = Seq.tabulate (8) (x => x.toLong)

  // 17 bytes long.
  val small = Seq.tabulate (2) (x => x.toLong)

  private def setup (
    f: => Async [Unit]
  ) (implicit
    files: FileSystem,
    scheduler: StubScheduler
  ): DiskAgent = {
    implicit val recovery = new RecoveryAgent
    val launch = recovery.reattach().expectPass()
    launch.checkpoint (f)
    launch.launch()
    val agent = launch.controller.asInstanceOf [DiskAgent]
    agent.attach (geom, "d1") .expectPass (Notification.unit)
    agent
  }

  private def recover (
    f: => Async [Unit]
  ) (implicit
    files: FileSystem,
    scheduler: StubScheduler
  ): DiskAgent = {
    implicit val recovery = new RecoveryAgent
    recovery.replay (desc) (_ => ())
    val launch = recovery.reattach ("d1") .expectPass()
    launch.checkpoint (f)
    launch.launch()
    scheduler.run()
    launch.controller.asInstanceOf [DiskAgent]
  }

  "The Checkpointer" should "invoke registered checkpointers" in {
    implicit val files = new StubFileSystem
    files.create ("d1", geom.diskBytes.toInt, geom.blockBits)
    implicit val scheduler = StubScheduler.random()

    var invoked = 0
    val controller = setup (supply (invoked += 1))

    controller.checkpoint().expectPass()
    assert (invoked == 1)
  }

  it should "queue and collapse requests" in {
    implicit val files = new StubFileSystem
    files.create ("d1", geom.diskBytes.toInt, geom.blockBits)
    implicit val scheduler = StubScheduler.random()

    var invoked = 0
    val controller = setup (supply (invoked += 1))

    // First request runs.
    val cb1 = controller.checkpoint().capture()
    // Second request is queued.
    val cb2 = controller.checkpoint().capture()
    // Third request is queued and run with second.
    controller.checkpoint().expectPass()
    cb1.assertInvoked()
    cb2.assertInvoked()
    assert (invoked == 2)
  }

  it should "be triggered by writing the threshold of bytes" in {
    implicit val files = new StubFileSystem
    files.create ("d1", 0, 1 << 14)
    implicit val scheduler = StubScheduler.random()

    val captor = AsyncCaptor [Unit]
    val controller = setup (captor.start())
    import controller.disk

    // Expect the first record doesn't trigger a checkpoint.
    desc.record (large) .expectPass()
    assert (captor.outstanding == 0)

    // Expect the second record does trigger one.
    desc.record (large) .expectPass()
    assert (captor.outstanding == 1)

    // Complete the checkpoint; expect no immediate subsequent checkpoint.
    captor.pass (())
    assert (captor.outstanding == 0)

    // Pager logged a record after checkpoint; it shouldn't trigger another one.
    desc.record (small) .expectPass()
    assert (captor.outstanding == 0)

    // Expect that another large record will trigger one.
    desc.record (large) .expectPass()
    assert (captor.outstanding == 1)

    // Complete the checkpoint; expect no immediate subsequent checkpoint.
    captor.pass (())
    assert (captor.outstanding == 0)
  }

  it should "be triggered by replaying the threshold of bytes" in {
    implicit val files = new StubFileSystem
    files.create ("d1", geom.diskBytes.toInt, geom.blockBits)

    {
      implicit val scheduler = StubScheduler.random()

      val captor = AsyncCaptor [Unit]
      val controller = setup (captor.start())
      import controller.disk

      // Expect the first record doesn't trigger a checkpoint.
      desc.record (large) .expectPass()
      assert (captor.outstanding == 0)

      // Expect the second record does trigger one.
      desc.record (large) .expectPass()
      assert (captor.outstanding == 1)

      // Crash during the checkpoint.
    }

    {
      implicit val scheduler = StubScheduler.random()

      val captor = AsyncCaptor [Unit]
      val controller = recover (captor.start())

      // Expect replay to retrigger the checkpoint.
      assert (captor.outstanding == 1)
    }}}
