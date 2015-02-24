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
import scala.util.Random

import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import org.scalatest.FlatSpec


class DispatcherSpec extends FlatSpec {

  "The Dispatcher" should "queue one message for a later receiver" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int]
    dsp.send (1)
    val captor = dsp.receive().capture()
    captor.expectPass ((1, Seq (1)))
    assert (dsp.isEmpty)
  }

  it should "deliver one message to an earlier receiver" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int]
    val captor = dsp.receive().capture()
    dsp.send (1)
    captor.expectPass ((1, Seq (1)))
    assert (dsp.isEmpty)
  }

  it should "queue several messages for a later receiver" in {
    implicit val schedule = StubScheduler.random()
    val dsp = new Dispatcher [Int]
    dsp.send (1)
    dsp.send (2)
    val captor = dsp.receive().capture()
    captor.expectPass ((1, Seq (1, 2)))
    assert (dsp.isEmpty)
  }

  it should "queue one batch of messages for a later receiver" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int]
    dsp.send (UnrolledBuffer (1, 2, 3))
    val captor = dsp.receive().capture()
    captor.expectPass ((1, Seq (1, 2, 3)))
    assert (dsp.isEmpty)
  }

  it should "deliver one batch of messages to an earlier receiver" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int]
    val captor = dsp.receive().capture()
    dsp.send (UnrolledBuffer (1, 2, 3))
    captor.expectPass ((1, Seq (1, 2, 3)))
    assert (dsp.isEmpty)
  }

  it should "queue several batches of messages for a later receiver" in {
    implicit val schedule = StubScheduler.random()
    val dsp = new Dispatcher [Int]
    dsp.send (UnrolledBuffer (1, 2))
    dsp.send (UnrolledBuffer (3, 4))
    val captor = dsp.receive().capture()
    captor.expectPass ((1, Seq (1, 2, 3, 4)))
    assert (dsp.isEmpty)
  }

  it should "queue a message and a message batch for a later receiver" in {
    implicit val schedule = StubScheduler.random()
    val dsp = new Dispatcher [Int]
    dsp.send (1)
    dsp.send (UnrolledBuffer (2, 3))
    val captor = dsp.receive().capture()
    captor.expectPass ((1, Seq (1, 2, 3)))
    assert (dsp.isEmpty)
  }

  it should "queue a message batch and a message for a later receiver" in {
    implicit val schedule = StubScheduler.random()
    val dsp = new Dispatcher [Int]
    dsp.send (UnrolledBuffer (1, 2))
    dsp.send (3)
    val captor = dsp.receive().capture()
    captor.expectPass ((1, Seq (1, 2, 3)))
    assert (dsp.isEmpty)
  }

  it should "queue several receievers, and deliver messages immediately" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int]
    val c1 = dsp.receive().capture()
    val c2 = dsp.receive().capture()
    dsp.send (1)
    dsp.send (2)
    c1.expectPass ((1, Seq (1)))
    c2.expectPass ((2, Seq (2)))
    assert (dsp.isEmpty)
  }}
