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

package com.treode.async

import com.treode.async.implicits._
import com.treode.async.stubs.CallbackCaptor
import org.scalatest.FlatSpec

class PairLatchSpec extends FlatSpec {

  class DistinguishedException extends Exception

  def pair [A, B] (cb: Callback [(A, B)]): (Callback [A], Callback [B]) = {
    val t = new PairLatch (cb)
    (t.cbA, t.cbB)
  }

  "The PairLatch" should "release after a and b are set" in {
    val cb = CallbackCaptor [(Int, Int)]
    val (la, lb) = pair [Int, Int] (cb)
    cb.assertNotInvoked()
    la.pass (1)
    cb.assertNotInvoked()
    lb.pass (2)
    assertResult ((1, 2)) (cb.assertPassed())
  }

  it should "reject two sets on a" in {
    val cb = CallbackCaptor [(Int, Int)]
    val (la, _) = pair [Int, Int] (cb)
    la.pass (1)
    intercept [Exception] (la.pass (0))
  }

  it should "reject two sets on b" in {
    val cb = CallbackCaptor [(Int, Int)]
    val (_, lb) = pair[Int, Int]  (cb)
    lb.pass (2)
    intercept [Exception] (lb.pass (0))
  }

  it should "release after a pass on b and a fail on a" in {
    val cb = CallbackCaptor [(Int, Int)]
    val (la, lb) = pair [Int, Int] (cb)
    cb.assertNotInvoked()
    la.fail (new DistinguishedException)
    cb.assertNotInvoked()
    lb.pass (2)
    cb.assertFailed [DistinguishedException]
  }

  it should "release after a pass on a and a fail on b" in {
    val cb = CallbackCaptor [(Int, Int)]
    val (la, lb) = pair [Int, Int] (cb)
    cb.assertNotInvoked()
    la.pass (1)
    cb.assertNotInvoked()
    lb.fail (new DistinguishedException)
    cb.assertFailed [DistinguishedException]
  }

  it should "release after a fail on a and b" in {
    val cb = CallbackCaptor [(Int, Int)]
    val (la, lb) = pair [Int, Int] (cb)
    cb.assertNotInvoked()
    la.fail (new DistinguishedException)
    cb.assertNotInvoked()
    lb.fail (new DistinguishedException)
    cb.assertFailed [MultiException]
  }}
