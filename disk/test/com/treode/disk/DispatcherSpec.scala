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
import com.treode.async.stubs.implicits._
import org.scalatest.FlatSpec


class DispatcherSpec extends FlatSpec {

  "The Dispatcher" should "send one message to a later receiver" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (0)
    dsp.send (1)
    val captor = dsp.receive().capture()
    captor.expectPass ((1, Seq (1)))
  }

  it should "send one message to an earlier receiver" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (0)
    val captor = dsp.receive().capture()
    dsp.send (1)
    captor.expectPass ((1, Seq (1)))
  }

  it should "queue several messages to a receiver" in {
    implicit val schedule = StubScheduler.random()
    val dsp = new Dispatcher [Int] (0)
    dsp.send (1)
    dsp.send (2)
    dsp.send (3)
    val captor = dsp.receive().capture()
    captor.expectPass ((1, Seq (1,2,3)))
  }

  it should "attempt to send multiple messages with only one receiver" in {
    implicit val schedule = StubScheduler.random()
    val dsp = new Dispatcher [Int] (0)
    val captor = dsp.receive().capture()
    dsp.send (1)
    dsp.send (2)
    captor.expectPass ((1, Seq(1)))
  }

  it should "create several receievers, each should receive a single message " in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (0)
    val captor = dsp.receive().capture()
    val captor2 = dsp.receive().capture()
    dsp.send (1)
    dsp.send (2)
    dsp.send (3)
    captor.expectPass ((1, Seq (1)))
    captor2.expectPass ((2, Seq (2)))
  
  }

  it should "have two messages immediately get picked up by receivers, leaving one messaged queued and later received" in {
    implicit val scheduler = StubScheduler.random()
    val dsp = new Dispatcher [Int] (0)
    val captor = dsp.receive().capture()
    val captor2 = dsp.receive().capture()
    dsp.send (1)
    dsp.send (2)
    dsp.send (3)
    captor.expectPass ((1, Seq (1)))
    captor2.expectPass ((2, Seq (2)))
    val captor3 = dsp.receive().capture()
    captor3.expectPass ((3, Seq (3)))
  }}
