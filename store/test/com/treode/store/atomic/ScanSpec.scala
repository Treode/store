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

package com.treode.store.atomic

import com.treode.async.stubs.StubScheduler
import com.treode.async.stubs.implicits._
import com.treode.cluster.stubs.StubNetwork
import com.treode.store._
import org.scalatest.FlatSpec

import AtomicTestTools._
import Bound.{Exclusive, Inclusive}
import Fruits._
import Window.{Between, Latest, Through}

class ScanSpec extends FlatSpec {

  val EMPTY =TableId (0x67)
  val SHORT = TableId (0xFD)
  val LONG = TableId (0xBE)

  private def setup (populate: Boolean) = {

    implicit val (random, scheduler, network) = newKit()

    val hs = Seq.fill (3) (StubAtomicHost .install() .expectPass())
    val Seq (h1, h2, h3) = hs
    for (h <- hs)
      h.setAtlas (settled (h1, h2, h3))

    h1.putCells (SHORT, Apple##1::1, Banana##1::1)
    h2.putCells (SHORT, Banana##1::1, Grape##1::1)
    h3.putCells (SHORT, Apple##1::1, Grape##1::1)

    h1.putCells (LONG, Apple##2::2, Apple##1::1, Grape##2::2, Grape##1::1)
    h2.putCells (LONG, Apple##3::3, Apple##2::2, Grape##3::3, Grape##1::1)
    h3.putCells (LONG, Apple##3::3, Apple##1::1, Grape##3::3, Grape##2::2)

    (random, scheduler, network, h1)
  }

  "Scan" should "handle an empty table" in {
    implicit val (random, scheduler, network, host) = setup (false)
    assertCells () {
      host.scan (EMPTY, 0, Bound.firstKey, Window.all, Slice.all, Batch.suggested)
    }}

  it should "handle a non-empty table" in {
    implicit val (random, scheduler, network, host) = setup (false)
    assertCells (Apple##1::1, Banana##1::1, Grape##1::1) {
      host.scan (SHORT, 0, Bound.firstKey, Window.all, Slice.all, Batch.suggested)
    }}

  it should "handle an inclusive start position" in {
    implicit val (random, scheduler, network, host) = setup (false)
    assertCells (Banana##1::1, Grape##1::1) {
      host.scan (SHORT, 0, Inclusive (Key (Banana, 1)), Window.all, Slice.all, Batch.suggested)
    }}

  it should "handle an exclusive start position" in {
    implicit val (random, scheduler, network, host) = setup (false)
    assertCells (Banana##1::1, Grape##1::1) {
      host.scan (SHORT, 0, Exclusive (Key (Apple, 1)), Window.all, Slice.all, Batch.suggested)
    }}

  it should "handle a filter" in {
    implicit val (random, scheduler, network, host) = setup (false)
    assertCells (Apple##1::1, Grape##1::1) {
      host.scan (LONG, 0, Bound.firstKey, Latest (1, true), Slice.all, Batch.suggested)
    }
    assertCells (Apple##2::2, Grape##2::2) {
      host.scan (LONG, 0, Bound.firstKey, Latest (2, true), Slice.all, Batch.suggested)
    }
    assertCells (Apple##3::3, Grape##3::3) {
      host.scan (LONG, 0, Bound.firstKey, Latest (3, true), Slice.all, Batch.suggested)
    }}

  it should "return only a slice" in {
    implicit val (random, scheduler, network, host) = setup (false)
    assertCells () {
      host.scan (LONG, 0, Bound.firstKey, Window.all, Slice (0, 4), Batch.suggested)
    }
    assertCells (Grape##3::3, Grape##2::2, Grape##1::1) {
      host.scan (LONG, 0, Bound.firstKey, Window.all, Slice (1, 4), Batch.suggested)
    }
    assertCells () {
      host.scan (LONG, 0, Bound.firstKey, Window.all, Slice (2, 4), Batch.suggested)
    }
    assertCells (Apple##3::3, Apple##2::2, Apple##1::1) {
      host.scan (LONG, 0, Bound.firstKey, Window.all, Slice (3, 4), Batch.suggested)
    }}}
