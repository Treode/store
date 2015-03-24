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

import com.treode.async.Callback
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.buffer.PagedBuffer
import org.scalatest.FlatSpec
import org.scalatest.prop.PropertyChecks

import PropertyChecks._

class SocketSpec extends FlatSpec {

  def mkSocket = {
    implicit val scheduler = StubScheduler.random()
    val async = new AsyncSocketMock
    val socket = new Socket (async)
    val buffer = PagedBuffer (5)
    (scheduler, async, socket, buffer)
  }

  "AsyncSocket.flush" should "handle an empty buffer" in {
    implicit val (scheduler, _, socket, buffer) = mkSocket
    socket.flush (buffer) .expectPass()
  }

  it should "flush an int" in {
    implicit val (scheduler, async, socket, buffer) = mkSocket
    buffer.writeInt (0)
    async.expectWrite (0, 4)
    val cb = socket.flush (buffer) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (4)
    cb.assertPassed()
  }

  it should "loop to flush an int" in {
    implicit val (scheduler, async, socket, output) = mkSocket
    output.writeInt (0)
    async.expectWrite (0, 4)
    async.expectWrite (2, 4)
    val cb = socket.flush (output) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (2)
    cb.assertNotInvoked()
    async.completeLast (2)
    cb.assertPassed()
  }

  it should "handle socket close" in {
    implicit val (scheduler, async, socket, output) = mkSocket
    output.writeInt (0)
    async.expectWrite (0, 4)
    val cb = socket.flush (output) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (-1)
    cb.assertFailed [Exception]
  }

  "AsyncSocket.fill" should "handle a request for 0 bytes" in {
    implicit val (scheduler, _, socket, input) = mkSocket
    socket.fill (input, 0) .expectPass()
  }

  it should "handle a request for bytes available at the beginning" in {
    implicit val (scheduler, _, socket, input) = mkSocket
    input.writePos = 4
    socket.fill (input, 4) .expectPass()
  }

  it should "fill needed bytes with an empty buffer" in {
    implicit val (scheduler, async, socket, input) = mkSocket
    async.expectRead (0, 32)
    val cb = socket.fill (input, 4) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (4)
    cb.assertPassed()
  }

  it should "loop to fill needed bytes within a page" in {
    implicit val (scheduler, async, socket, input) = mkSocket
    async.expectRead (0, 32)
    async.expectRead (2, 32)
    val cb = socket.fill (input, 4) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (2)
    cb.assertNotInvoked()
    async.completeLast (2)
    cb.assertPassed()
  }

  it should "fill needed bytes with some at the beginning" in {
    implicit val (scheduler, async, socket, input) = mkSocket
    input.writePos = 2
    async.expectRead (2, 32)
    val cb = socket.fill (input, 4) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (2)
    cb.assertPassed()
  }

  it should "handle a request for bytes available in the middle" in {
    implicit val (scheduler, async, socket, input) = mkSocket
    input.writePos = 4
    input.readPos = 4
    async.expectRead (4, 32)
    val cb = socket.fill (input, 4) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (4)
    cb.assertPassed()
  }

  it should "fill needed bytes with some in the middle and space after" in {
    implicit val (scheduler, async, socket, input) = mkSocket
    input.writePos = 6
    input.readPos = 4
    async.expectRead (6, 32)
    val cb = socket.fill (input, 4) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (2)
    cb.assertPassed()
  }

  it should "repeat to fill needed bytes across pages" in {
    implicit val (scheduler, async, socket, input) = mkSocket
    input.writePos = 30
    input.readPos = 26
    async.expectRead (30, 32)
    async.expectRead (0, 32)
    val cb = socket.fill (input, 8) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (2)
    cb.assertNotInvoked()
    async.completeLast (6)
    cb.assertPassed()
  }

  it should "handle socket close" in {
    implicit val (scheduler, async, socket, input) = mkSocket
    async.expectRead (0, 32)
    val cb = socket.fill (input, 4) .capture()
    scheduler.run()
    cb.assertNotInvoked()
    async.completeLast (-1)
    cb.assertFailed [Exception]
  }

  it should "flush and fill" in {
    forAll ("seed") { seed: Int =>
      implicit val random = new Random (seed)
      implicit val scheduler = StubScheduler.random (random)
      val socket = new Socket (new AsyncSocketStub (random))
      val data = Array.fill (100) (random.nextInt)
      val buffer = PagedBuffer (5)
      for (i <- data)
        buffer.writeVarInt (i)
      val length = buffer.writePos
      socket.flush (buffer) .expectPass()
      buffer.clear()
      socket.fill (buffer, length) .expectPass()
      for (i <- data)
        assertResult (i) (buffer.readVarInt())
    }}}
