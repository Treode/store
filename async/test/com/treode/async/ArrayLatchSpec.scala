package com.treode.async

import org.scalatest.FlatSpec

class ArrayLatchSpec extends FlatSpec {

  class DistinguishedException extends Exception

  "The ArrayLatchSpec" should "release immediately for count==0" in {
    val cb = new CallbackCaptor [Array [Int]]
    val ltch = Callback.collate (0, cb)
    expectResult (Seq [Int] ()) (cb.passed.toSeq)
  }

  it should "reject extra releases" in {
    val cb = new CallbackCaptor [Array [Int]]
    val ltch = Callback.collate (0, cb)
    expectResult (Seq [Int] ()) (cb.passed.toSeq)
    intercept [Exception] (ltch (0, 0))
  }

  it should "release after one pass for count==1" in {
    val cb = new CallbackCaptor [Array [Int]]
    val ltch = Callback.collate (1, cb)
    assert (!cb.wasInvoked)
    ltch (0, 1)
    expectResult (Seq (1)) (cb.passed.toSeq)
  }

  it should "release after one fail for count==1" in {
    val cb = new CallbackCaptor [Array [Int]]
    val ltch = Callback.collate (1, cb)
    assert (!cb.wasInvoked)
    ltch.fail (new DistinguishedException)
    assert (cb.failed.isInstanceOf [DistinguishedException])
  }

  it should "release after two passes for count==2" in {
    val cb = new CallbackCaptor [Array [Int]]
    val ltch = Callback.collate (2, cb)
    assert (!cb.wasInvoked)
    ltch (0, 1)
    assert (!cb.wasInvoked)
    ltch (1, 2)
    expectResult (Seq (1, 2)) (cb.passed.toSeq)
  }

  it should "release after two reversed passes for count==2" in {
    val cb = new CallbackCaptor [Array [Int]]
    val ltch = Callback.collate (2, cb)
    assert (!cb.wasInvoked)
    ltch (1, 2)
    assert (!cb.wasInvoked)
    ltch (0, 1)
    expectResult (Seq (1, 2)) (cb.passed.toSeq)
  }

  it should "release after a pass and a fail for count==2" in {
    val cb = new CallbackCaptor [Array [Int]]
    val ltch = Callback.collate (2, cb)
    assert (!cb.wasInvoked)
    ltch (0, 0)
    assert (!cb.wasInvoked)
    ltch.fail (new DistinguishedException)
    assert (cb.failed.isInstanceOf [DistinguishedException])
  }

  it should "release after two fails for count==2" in {
    val cb = new CallbackCaptor [Array [Int]]
    val ltch = Callback.collate (2, cb)
    assert (!cb.wasInvoked)
    ltch.fail (new Exception)
    assert (!cb.wasInvoked)
    ltch.fail (new Exception)
    assert (cb.failed.isInstanceOf [MultiException])
  }}
