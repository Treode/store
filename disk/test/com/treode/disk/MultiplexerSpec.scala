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

import scala.collection.mutable.UnrolledBuffer

import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import org.scalatest.FlatSpec

import MultiplexerTestTools._

class MultiplexerSpec extends FlatSpec {

  "The Multiplexer" should "relay messages from the dispatcher" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int]
    val mplx = new Multiplexer (dsp)
    dsp.send (1)
    mplx.expect (1)
    mplx.expectNone()
  }

  it should "return open rejects to the dispatcher" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int]
    val mplx = new Multiplexer (dsp)
    dsp.send (1)
    dsp.send (2)
    mplx.expect (1, 2)
    mplx.send (list (2))
    dsp.expect (2)
    mplx.expectNone()
  }

  it should "relay exclusive messages when available" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int]
    val mplx = new Multiplexer (dsp)
    mplx.send (2)
    mplx.expect (2)
    mplx.expectNone()
  }

  it should "recycle exclusive messages rejected by an earlier receptor" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int]
    val rcpt1 = dsp.receptor()
    val mplx = new Multiplexer (dsp)
    val rcpt2 = mplx.receptor()
    scheduler.run()
    mplx.send (2)
    rcpt2.expect (2)
    mplx.send (list (2))
    mplx.expect (2)
    rcpt1.expectNone()
  }

  it should "recycle exclusive messages rejected by a later receptor" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int]
    val rcpt1 = dsp.receptor()
    val mplx = new Multiplexer (dsp)
    mplx.send (2)
    mplx.expect (2)
    mplx.send (list (2))
    mplx.expect (2)
    rcpt1.expectNone()
  }

  it should "close immediately when a receiver is waiting" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int]
    val mplx = new Multiplexer (dsp)
    val rcpt = mplx.receptor()
    mplx.close() .expectPass()
    dsp.send (1)
    rcpt.expectNone()
    dsp.expect (1)
  }

  it should "delay close when a until a receiver is waiting" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int]
    val mplx = new Multiplexer (dsp)
    val cb = mplx.close().capture()
    scheduler.run()
    cb.assertNotInvoked()
    dsp.send (1)
    scheduler.run()
    cb.assertNotInvoked()
    val rcpt = mplx.receptor()
    scheduler.run()
    cb.assertPassed()
    rcpt.expectNone()
    dsp.expect (1)
  }

  it should "recycle rejects until accepted" in {

    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int]

    var received = Set.empty [Int]

    /* Bits of 0xF indicate open (0) or exclusive (non-zero).
     * Accept upto 5 messages m where (m & 0xF0 == i)
     * Ensure m was not already accepted and (m & 0xF == 0 || m & 0xF == i).
     */
    def receive (mplx: Multiplexer [Int], i: Int) (num: Long, msgs: UnrolledBuffer [Int]) {
      assert (!(msgs exists (received contains _)))
      assert (msgs forall (m => ((m & 0xF) == 0 || (m & 0xF) == i)))
      val (accepts, rejects) = msgs.partition (m => (m & 0xF0) == (i << 4))
      mplx.send ((accepts drop 5) ++ rejects)
      received ++= accepts take 5
      mplx.receive (receive (mplx, i))
    }

    var expected = Set.empty [Int]
    val count = 100
    for (i <- 0 until count) {
      dsp.send ((i << 8) | 0x10)
      expected += (i << 8) | 0x10
      dsp.send ((i << 8) | 0x20)
      expected += (i << 8) | 0x20
      dsp.send ((i << 8) | 0x30)
      expected += (i << 8) | 0x30
    }
    for (i <- 1 to 3) {
      val mplx = new Multiplexer (dsp)
      dsp.receive (receive (mplx, i))
      for (j <- 0 until count) {
        mplx.send ((j << 8) | (i << 4) | i)
        expected += (j << 8) | (i << 4) | i
      }}

    scheduler.run()

    assertResult (expected) (received)
  }}
