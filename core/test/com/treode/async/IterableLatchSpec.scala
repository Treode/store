/*
 * Copyright 2015 Treode, Inc.
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

package com.treode.async

import com.treode.async.implicits._
import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import org.scalatest.FlatSpec

import Async.supply

class IterableLatchSpec extends FlatSpec {

  def map (xs: Int*): Async [Seq [Int]] =
    xs.zipWithIndex.latch.map (i => supply (i._1 + 1))

  def filter (xs: Int*): Async [Seq [Int]] =
    xs.zipWithIndex.latch.filter (i => (i._1 % 2) == 0) .map (i => supply (i._1))

  "The IterableLatch" should "handle an empty sequence" in {
    implicit val scheduler = StubScheduler.random()
    map().expectSeq()
  }

  it should "handle a sequence of one item" in {
    implicit val scheduler = StubScheduler.random()
    map (1) .expectSeq (2)
  }

  it should "handle a sequence of two items" in {
    implicit val scheduler = StubScheduler.random()
    map (1, 2) .expectSeq (2, 3)
  }

  it should "handle a sequence of three items" in {
    implicit val scheduler = StubScheduler.random()
    map (1, 2, 3) .expectSeq (2, 3, 4)
  }

  it should "handle a sequence filtered to no items" in {
    implicit val scheduler = StubScheduler.random()
    filter (1, 3, 5) .expectSeq()
  }

  it should "handle a sequence filtered to one item" in {
    implicit val scheduler = StubScheduler.random()
    filter (1, 2, 3) .expectSeq (2)
  }

  it should "handle a sequence filtered to everything" in {
    implicit val scheduler = StubScheduler.random()
    filter (2, 4, 6) .expectSeq (2, 4, 6)
  }}
