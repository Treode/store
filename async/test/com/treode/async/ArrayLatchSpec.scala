package com.treode.async

import org.scalatest.FlatSpec

class ArrayLatchSpec extends FlatSpec {

  class DistinguishedException extends Exception

  "The ArrayLatch" should "release immediately for count==0" in {
    val cb = CallbackCaptor [Array [Int]]
    val ltch = Latch.array (0, cb)
    expectResult (Seq [Int] ()) (cb.passed.toSeq)
  }

  it should "reject extra releases" in {
    val cb = CallbackCaptor [Array [Int]]
    val ltch = Latch.array (0, cb)
    expectResult (Seq [Int] ()) (cb.passed.toSeq)
    intercept [Exception] (ltch.pass (0, 0))
  }

  it should "release after one pass for count==1" in {
    val cb = CallbackCaptor [Array [Int]]
    val ltch = Latch.array (1, cb)
    cb.expectNotInvoked()
    ltch.pass (0, 1)
    expectResult (Seq (1)) (cb.passed.toSeq)
  }

  it should "release after one fail for count==1" in {
    val cb = CallbackCaptor [Array [Int]]
    val ltch = Latch.array (1, cb)
    cb.expectNotInvoked()
    ltch.fail (new DistinguishedException)
    cb.failed [DistinguishedException]
  }

  it should "release after two passes for count==2" in {
    val cb = CallbackCaptor [Array [Int]]
    val ltch = Latch.array (2, cb)
    cb.expectNotInvoked()
    ltch.pass (0, 1)
    cb.expectNotInvoked()
    ltch.pass (1, 2)
    expectResult (Seq (1, 2)) (cb.passed.toSeq)
  }

  it should "release after two reversed passes for count==2" in {
    val cb = CallbackCaptor [Array [Int]]
    val ltch = Latch.array (2, cb)
    cb.expectNotInvoked()
    ltch.pass (1, 2)
    cb.expectNotInvoked()
    ltch.pass (0, 1)
    expectResult (Seq (1, 2)) (cb.passed.toSeq)
  }

  it should "release after a pass and a fail for count==2" in {
    val cb = CallbackCaptor [Array [Int]]
    val ltch = Latch.array (2, cb)
    cb.expectNotInvoked()
    ltch.pass (0, 0)
    cb.expectNotInvoked()
    ltch.fail (new DistinguishedException)
    cb.failed [DistinguishedException]
  }

  it should "release after two fails for count==2" in {
    val cb = CallbackCaptor [Array [Int]]
    val ltch = Latch.array (2, cb)
    cb.expectNotInvoked()
    ltch.fail (new Exception)
    cb.expectNotInvoked()
    ltch.fail (new Exception)
    cb.failed [MultiException]
  }}
