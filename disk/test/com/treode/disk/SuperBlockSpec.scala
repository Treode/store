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

import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.buffer.PagedBuffer
import org.scalatest.FlatSpec

import DiskTestTools._

class SuperBlockSpec extends FlatSpec {

  "SuperBlock.write" should "reject oversized superblocks" in {

    // Setup the config with a small space for the superblock.
    implicit val scheduler = StubScheduler.random()
    val config = DiskTestConfig (
        superBlockBits = 4,
        maximumRecordBytes = 1<<6,
        maximumPageBytes = 1<<6)
    val boot = BootBlock (sysid, 0, 0, Set.empty)
    val geom = DriveGeometry.test (blockBits=4) (config)
    val free = IntSet()
    val superb = new SuperBlock (0, boot, geom, false, free, 0)

    // Write something known to the file.
    val buf = PagedBuffer (12)
    for (i <- 0 until 1024)
      buf.writeInt (i)
    val file = StubFile (1<<20, geom.blockBits)
    file.flush (buf, 0) .expectPass()

    // Check that the write throws an exception.
    SuperBlock.write (superb, file) (config) .fail [SuperblockOverflowException]

    // Check that the file has not been overwritten.
    buf.clear()
    file.fill (buf, 0, 1024) .expectPass()
    for (i <- 0 until 1024)
      assertResult (i) (buf.readInt())
  }}
