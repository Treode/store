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

package com.treode.async.io

import scala.util.Random

import com.google.common.hash.Hashing
import com.treode.async.Callback
import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.buffer.PagedBuffer
import com.treode.pickle.Picklers
import org.scalatest.FlatSpec
import org.scalatest.prop.PropertyChecks

import PropertyChecks._

class FileSpec extends FlatSpec {

  def mkFile = {
    implicit val scheduler = StubScheduler.random()
    val async = new AsyncFileMock
    val file = new File (async)
    (scheduler, async, file)
  }

  "File.flush" should "handle an empty buffer" in {
    implicit val (scheduler, _, file) = mkFile
    val buffer = PagedBuffer (5)
    file.flush (buffer, 0) .expectPass()
  }

  it should "flush an int" in {
    implicit val (scheduler, async, file) = mkFile
    val buffer = PagedBuffer (5)
    buffer.writeInt (0)
    async.expectWrite (0, 0, 4)
    val cb = file.flush (buffer, 0) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (4)
    cb.assertPassed()
  }

  it should "loop to flush an int" in {
    implicit val (scheduler, async, file) = mkFile
    val output = PagedBuffer (5)
    output.writeInt (0)
    async.expectWrite (0, 0, 4)
    async.expectWrite (2, 2, 4)
    val cb = file.flush (output, 0) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (2)
    cb.assertNotInvoked()
    async.completeLast (2)
    cb.assertPassed()
  }

  it should "handle file close" in {
    implicit val (scheduler, async, file) = mkFile
    val output = PagedBuffer (5)
    output.writeInt (0)
    async.expectWrite (0, 0, 4)
    val cb = file.flush (output, 0) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (-1)
    cb.assertFailed [Exception]
  }

  "File.fill" should "handle a request for 0 bytes" in {
    implicit val (scheduler, _, file) = mkFile
    val input = PagedBuffer (5)
    file.fill (input, 0, 0) .expectPass()
  }

  it should "handle a request for bytes available at the beginning" in {
    implicit val (scheduler, _, file) = mkFile
    val input = PagedBuffer (5)
    input.writePos = 4
    file.fill (input, 0, 4) .expectPass()
  }

  it should "fill needed bytes with an empty buffer" in {
    implicit val (scheduler, async, file) = mkFile
    val input = PagedBuffer (5)
    async.expectRead (0, 0, 32)
    val cb = file.fill (input, 0, 4) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (4)
    cb.assertPassed()
  }

  it should "loop to fill needed bytes within a page" in {
    implicit val (scheduler, async, file) = mkFile
    val input = PagedBuffer (5)
    async.expectRead (0, 0, 32)
    async.expectRead (2, 2, 32)
    val cb = file.fill (input, 0, 4) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (2)
    cb.assertNotInvoked()
    async.completeLast (2)
    cb.assertPassed()
  }

  it should "fill needed bytes with some at the beginning" in {
    implicit val (scheduler, async, file) = mkFile
    val input = PagedBuffer (5)
    input.writePos = 2
    async.expectRead (2, 2, 32)
    val cb = file.fill (input, 0, 4) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (2)
    cb.assertPassed()
  }

  it should "handle a request for bytes available in the middle" in {
    implicit val (scheduler, async, file) = mkFile
    val input = PagedBuffer (5)
    input.writePos = 4
    input.readPos = 0
    async.expectRead (0, 4, 32)
    file.fill (input, 0, 4) .expectPass()
  }

  it should "fill needed bytes with some in the middle and space after" in {
    implicit val (scheduler, async, file) = mkFile
    val input = PagedBuffer (5)
    input.writePos = 6
    input.readPos = 4
    async.expectRead (2, 6, 32)
    val cb = file.fill (input, 0, 4) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (2)
    cb.assertPassed()
  }

  it should "repeat to fill needed bytes across pages" in {
    implicit val (scheduler, async, file) = mkFile
    val input = PagedBuffer (5)
    input.writePos = 30
    input.readPos = 26
    async.expectRead (4, 30, 32)
    async.expectRead (6, 0, 32)
    val cb = file.fill (input, 0, 8) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (2)
    cb.assertNotInvoked()
    async.completeLast (6)
    cb.assertPassed()
  }

  it should "handle file close" in {
    implicit val (scheduler, async, file) = mkFile
    val input = PagedBuffer (5)
    async.expectRead (0, 0, 32)
    val cb = file.fill (input, 0, 4) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (-1)
    cb.assertFailed [Exception]
  }

  it should "align and fill needed bytes with an empty buffer" in {
    implicit val (scheduler, async, file) = mkFile
    val input = PagedBuffer (5)
    async.expectRead (0, 0, 32)
    val cb = file.fill (input, 0, 4, 5) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (32)
    cb.assertPassed()
  }

  it should "align and fill needed bytes within a page" in {
    implicit val (scheduler, async, file) = mkFile
    val input = PagedBuffer (12)
    input.writePos = 32
    input.readPos = 30
    async.expectRead (32, 32, 4096)
    val cb = file.fill (input, 30, 4, 5) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (32)
    cb.assertPassed()
  }

  it should "align and fill needed bytes across pages" in {
    implicit val (scheduler, async, file) = mkFile
    val input = PagedBuffer (5)
    input.writePos = 32
    input.readPos = 30
    async.expectRead (32, 0, 32)
    val cb = file.fill (input, 30, 4, 5) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (32)
    cb.assertPassed()
  }

  it should "reject a misaligned buffer" in {
    implicit val (scheduler, async, file) = mkFile
    val input = PagedBuffer (5)
    input.writeInt (0)
    val e = file.fill (input, 0, 8, 5) .fail [IllegalArgumentException]
    assertResult ("requirement failed: Buffer writePos must be aligned.") (e.getMessage)
  }

  it should "reject an undersized buffer" in {
    implicit val (scheduler, async, file) = mkFile
    val input = PagedBuffer (5)
    val e = file.fill (input, 0, 4, 12) .fail [IllegalArgumentException]
    assertResult ("requirement failed: Buffer page size must accomodate alignment.") (e.getMessage)
  }

  "File.deframe" should "read from Pickler.frame" in {
    implicit val scheduler = StubScheduler.random()
    val file = StubFile (1 << 12, 0)
    val pickler = Picklers.seq (Picklers.int)
    val out = Seq.fill (23) (Random.nextInt)
    val buffer = PagedBuffer (12)
    pickler.frame (out, buffer)
    file.flush (buffer, 0) .expectPass()
    buffer.clear()
    file.deframe (buffer, 0) .expectPass()
    val in = pickler.unpickle (buffer)
    assertResult (out) (in)
  }

  it should "read from Pickler.frame with hashing" in {
    implicit val scheduler = StubScheduler.random()
    val file = StubFile (1 << 12, 0)
    val pickler = Picklers.seq (Picklers.int)
    val out = Seq.fill (23) (Random.nextInt)
    val buffer = PagedBuffer (12)
    pickler.frame (Hashing.crc32, out, buffer)
    file.flush (buffer, 0) .expectPass()
    buffer.clear()
    file.deframe (Hashing.crc32, buffer, 0) .expectPass()
    val in = pickler.unpickle (buffer)
    assertResult (out) (in)
  }

  it should "raise an error when the hash check fails" in {
    implicit val scheduler = StubScheduler.random()
    val file = StubFile (1 << 12, 0)
    val pickler = Picklers.seq (Picklers.int)
    val out = Seq.fill (23) (Random.nextInt)
    val buffer = PagedBuffer (12)
    pickler.frame (Hashing.crc32, out, buffer)
    val end = buffer.writePos
    buffer.writePos = 33
    buffer.writeInt (Random.nextInt)
    buffer.writePos = end
    file.flush (buffer, 0) .expectPass()
    buffer.clear()
    file.deframe (Hashing.crc32, buffer, 0) .fail [HashMismatchException]
  }

  it should "flush and fill" in {
    forAll ("seed") { seed: Int =>
      implicit val random = new Random (seed)
      implicit val scheduler = StubScheduler.random (random)
      val file = new File (new AsyncFileStub)
      val data = Array.fill (100) (random.nextInt)
      val buffer = PagedBuffer (5)
      for (i <- data)
        buffer.writeVarInt (i)
      val length = buffer.writePos
      file.flush (buffer, 0) .expectPass()
      buffer.clear()
      file.fill (buffer, 0, length) .expectPass()
      for (i <- data)
        assertResult (i) (buffer.readVarInt())
    }}}
