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

import scala.util.Random

import com.treode.async.stubs.StubScheduler
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.implicits._
import com.treode.buffer.PagedBuffer
import org.scalatest.FlatSpec

import DiskTestTools._

class PageLedgerSpec extends FlatSpec {

  "PageLedger.write" should "reject oversized ledgers" in {

    implicit val random = new Random
    implicit val scheduler = StubScheduler.random (random)
    implicit val config = DiskTestConfig()
    val geom = DriveGeometry.test()

    // Make a large ledger.
    val ledger = new PageLedger
    for (_ <- 0 until 256)
      ledger.add (random.nextInt, random.nextLong, random.nextGroup, 128)

    // Write something known to the file.
    val buf = PagedBuffer (12)
    for (i <- 0 until 1024)
      buf.writeInt (i)
    val file = StubFile (1<<20, 6)
    file.flush (buf, 0) .expectPass()

    // Check that the write throws an exception.
    PageLedger.write (ledger, file, geom, 0, 256) .expectFail [PageLedgerOverflowException]

    // Check that the file has not been overwritten.
    buf.clear()
    file.fill (buf, 0, 1024) .expectPass()
    for (i <- 0 until 1024)
      assertResult (i) (buf.readInt())
  }}
