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

import com.treode.async.Async
import com.treode.async.stubs.{AsyncCaptor, StubScheduler}
import com.treode.async.stubs.implicits._
import com.treode.async.io.stubs.StubFile
import com.treode.pickle.Picklers
import org.scalatest.FlatSpec

import Async.supply
import DiskTestTools._

class CheckpointerSpec extends FlatSpec {

  implicit val config = DiskTestConfig (checkpointBytes = 128)
  val geom = DriveGeometry.test()

  // We use arrays of longs for our sample log records.
  val desc = {
    import Picklers._
    RecordDescriptor (0x58, seq (fixedLong))
  }

  // 65 bytes long.
  val large = Seq.tabulate (8) (x => x.toLong)

  // 17 bytes long.
  val small = Seq.tabulate (2) (x => x.toLong)

  "It" should "be triggered by number of bytes between checkpoints" in {

    implicit val scheduler = StubScheduler.random()

    // Captor to hold the checkpoint indefinitely.
    val captor = AsyncCaptor [Unit]

    // Setup the disk system with a checkpoint hook.
    val file = StubFile (1 << 20, geom.blockBits)
    implicit val recovery = Disk.recover()
    implicit val launch = recovery.attachAndWait (("a", file, geom)) .expectPass()
    launch.checkpoint (captor.start())
    launch.launchAndPass()
    import launch.disk

    // Expect the first record doesn't trigger a checkpoint.
    desc.record (large) .expectPass()
    assertResult (0) (captor.outstanding)

    // Expect the second record does trigger one.
    desc.record (large) .expectPass()
    assertResult (1) (captor.outstanding)

    // Complete the checkpoint; expect no immediate subsequent checkpoint.
    captor.pass (())
    assertResult (0) (captor.outstanding)

    // Pager logged a after checkpoint; the small record shouldn't trigger another one.
    desc.record (small) .expectPass()
    assertResult (0) (captor.outstanding)

    // Expect that another large record will trigger one.
    desc.record (large) .expectPass()
    assertResult (1) (captor.outstanding)

    // Complete the checkpoint; expect no immediate subsequent checkpoint.
    captor.pass (())
    assertResult (0) (captor.outstanding)
  }

  it should "be triggered by number of bytes during a checkpoint" in {

    implicit val scheduler = StubScheduler.random()

    // Captor to hold the checkpoint indefinitely.
    val captor = AsyncCaptor [Unit]

    // Setup the disk system with a checkpoint hook.
    val file = StubFile (1 << 20, geom.blockBits)
    implicit val recovery = Disk.recover()
    implicit val launch = recovery.attachAndWait (("a", file, geom)) .expectPass()
    launch.checkpoint (captor.start())
    launch.launchAndPass()
    import launch.disk

    // Expect  two records to trigger the first checkpoint; hold it.
    desc.record (large) .expectPass()
    desc.record (large) .expectPass()
    assertResult (1) (captor.outstanding)

    // Expect additional records do not trigger a checkpoint, because the first is outstanding.
    desc.record (large) .expectPass()
    desc.record (large) .expectPass()
    assertResult (1) (captor.outstanding)

    // Complete the first checkpoint; expect an immediate subsequent checkpoint.
    captor.pass (())
    assertResult (1) (captor.outstanding)

    // Complete the second checkpoint; expect no immediate subsequent checkpoint.
    captor.pass (())
    assertResult (0) (captor.outstanding)
  }}
