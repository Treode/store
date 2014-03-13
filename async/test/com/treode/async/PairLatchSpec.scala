package com.treode.async

import org.scalatest.FlatSpec

import AsyncConversions._

class PairLatchSpec extends FlatSpec {

  class DistinguishedException extends Exception

  "The PairLatch" should "release after a and b are set" in {
    val cb = CallbackCaptor [(Int, Int)]
    val (la, lb) = Latch.pair [Int, Int] (cb)
    cb.expectNotInvoked()
    la.pass (1)
    cb.expectNotInvoked()
    lb.pass (2)
    expectResult ((1, 2)) (cb.passed)
  }

  it should "reject two sets on a" in {
    val cb = CallbackCaptor [(Int, Int)]
    val (la, lb) = Latch.pair [Int, Int] (cb)
    la.pass (1)
    intercept [Exception] (la.pass (0))
  }

  it should "reject two sets on b" in {
    val cb = CallbackCaptor [(Int, Int)]
    val (la, lb) = Latch.pair[Int, Int]  (cb)
    lb.pass (2)
    intercept [Exception] (lb.pass (0))
  }

  it should "release after a pass on b and a fail on a" in {
    val cb = CallbackCaptor [(Int, Int)]
    val (la, lb) = Latch.pair [Int, Int] (cb)
    cb.expectNotInvoked()
    la.fail (new DistinguishedException)
    cb.expectNotInvoked()
    lb.pass (2)
    cb.failed [DistinguishedException]
  }

  it should "release after a pass on a and a fail on b" in {
    val cb = CallbackCaptor [(Int, Int)]
    val (la, lb) = Latch.pair [Int, Int] (cb)
    cb.expectNotInvoked()
    la.pass (1)
    cb.expectNotInvoked()
    lb.fail (new DistinguishedException)
    cb.failed [DistinguishedException]
  }

  it should "release after a fail on a and b" in {
    val cb = CallbackCaptor [(Int, Int)]
    val (la, lb) = Latch.pair [Int, Int] (cb)
    cb.expectNotInvoked()
    la.fail (new DistinguishedException)
    cb.expectNotInvoked()
    lb.fail (new DistinguishedException)
    cb.failed [MultiException]
  }}
