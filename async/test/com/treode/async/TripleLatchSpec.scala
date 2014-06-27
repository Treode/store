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

import org.scalatest.FlatSpec

import com.treode.async.implicits._
import com.treode.async.stubs.CallbackCaptor

class TripleLatchSpec extends FlatSpec {

  class DistinguishedException extends Exception

  def triple [A, B, C] (cb: Callback [(A, B, C)]): (Callback [A], Callback [B], Callback [C]) = {
    val t = new TripleLatch (cb)
    (t.cbA, t.cbB, t.cbC)
  }

  "The TripleLatch" should "release after a, b and c are set" in {
    val cb = CallbackCaptor [(Int, Int, Int)]
    val (la, lb, lc) = triple [Int, Int, Int] (cb)
    cb.assertNotInvoked()
    la.pass (1)
    cb.assertNotInvoked()
    lb.pass (2)
    cb.assertNotInvoked()
    lc.pass (3)
    assertResult ((1, 2, 3)) (cb.passed)
  }

  it should "reject two sets on a" in {
    val cb = CallbackCaptor [(Int, Int, Int)]
    val (la, _, _) = triple [Int, Int, Int] (cb)
    la.pass (1)
    intercept [Exception] (la.pass (0))
  }

  it should "reject two sets on b" in {
    val cb = CallbackCaptor [(Int, Int, Int)]
    val (_, lb, _) = triple [Int, Int, Int] (cb)
    lb.pass (2)
    intercept [Exception] (lb.pass (0))
  }

  it should "reject two sets on c" in {
    val cb = CallbackCaptor [(Int, Int, Int)]
    val (_, _, lc) = triple [Int, Int, Int] (cb)
    lc.pass (4)
    intercept [Exception] (lc.pass (0))
  }

  it should "release after two passes but a fail on a" in {
    val cb = CallbackCaptor [(Int, Int, Int)]
    val (la, lb, lc) = triple [Int, Int, Int] (cb)
    cb.assertNotInvoked()
    la.fail (new DistinguishedException)
    cb.assertNotInvoked()
    lb.pass (2)
    cb.assertNotInvoked()
    lc.pass (3)
    cb.failed [DistinguishedException]
  }

  it should "release after two passes but a fail on b" in {
    val cb = CallbackCaptor [(Int, Int, Int)]
    val (la, lb, lc) = triple [Int, Int, Int] (cb)
    cb.assertNotInvoked()
    la.pass (1)
    cb.assertNotInvoked()
    lb.fail (new DistinguishedException)
    cb.assertNotInvoked()
    lc.pass (3)
    cb.failed [DistinguishedException]
  }

  it should "release after two passes but a fail on c" in {
    val cb = CallbackCaptor [(Int, Int, Int)]
    val (la, lb, lc) = triple [Int, Int, Int] (cb)
    cb.assertNotInvoked()
    la.pass (1)
    cb.assertNotInvoked()
    lb.pass (2)
    cb.assertNotInvoked()
    lc.fail (new DistinguishedException)
    cb.failed [DistinguishedException]
  }}
