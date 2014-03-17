package com.treode.async

import org.scalatest.FlatSpec

import AsyncConversions._

class TripleLatchSpec extends FlatSpec {

  class DistinguishedException extends Exception

  def triple [A, B, C] (cb: Callback [(A, B, C)]): (Callback [A], Callback [B], Callback [C]) = {
    val t = new TripleLatch (cb)
    (t.cbA, t.cbB, t.cbC)
  }

  "The TripleLatch" should "release after a, b and c are set" in {
    val cb = CallbackCaptor [(Int, Int, Int)]
    val (la, lb, lc) = triple [Int, Int, Int] (cb)
    cb.expectNotInvoked()
    la.pass (1)
    cb.expectNotInvoked()
    lb.pass (2)
    cb.expectNotInvoked()
    lc.pass (3)
    assertResult ((1, 2, 3)) (cb.passed)
  }

  it should "reject two sets on a" in {
    val cb = CallbackCaptor [(Int, Int, Int)]
    val (la, lb, lc) = triple [Int, Int, Int] (cb)
    la.pass (1)
    intercept [Exception] (la.pass (0))
  }

  it should "reject two sets on b" in {
    val cb = CallbackCaptor [(Int, Int, Int)]
    val (la, lb, lc) = triple [Int, Int, Int] (cb)
    lb.pass (2)
    intercept [Exception] (lb.pass (0))
  }

  it should "reject two sets on c" in {
    val cb = CallbackCaptor [(Int, Int, Int)]
    val (la, lb, lc) = triple [Int, Int, Int] (cb)
    lc.pass (4)
    intercept [Exception] (lc.pass (0))
  }

  it should "release after two passes but a fail on a" in {
    val cb = CallbackCaptor [(Int, Int, Int)]
    val (la, lb, lc) = triple [Int, Int, Int] (cb)
    cb.expectNotInvoked()
    la.fail (new DistinguishedException)
    cb.expectNotInvoked()
    lb.pass (2)
    cb.expectNotInvoked()
    lc.pass (3)
    cb.failed [DistinguishedException]
  }

  it should "release after two passes but a fail on b" in {
    val cb = CallbackCaptor [(Int, Int, Int)]
    val (la, lb, lc) = triple [Int, Int, Int] (cb)
    cb.expectNotInvoked()
    la.pass (1)
    cb.expectNotInvoked()
    lb.fail (new DistinguishedException)
    cb.expectNotInvoked()
    lc.pass (3)
    cb.failed [DistinguishedException]
  }

  it should "release after two passes but a fail on c" in {
    val cb = CallbackCaptor [(Int, Int, Int)]
    val (la, lb, lc) = triple [Int, Int, Int] (cb)
    cb.expectNotInvoked()
    la.pass (1)
    cb.expectNotInvoked()
    lb.pass (2)
    cb.expectNotInvoked()
    lc.fail (new DistinguishedException)
    cb.failed [DistinguishedException]
  }}
