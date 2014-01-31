package com.treode.async

import org.scalatest.FlatSpec

class MapLatchSpec extends FlatSpec {

  class DistinguishedException extends Exception

  "The MapLatch" should "release immediately for count==0" in {
    val cb = new CallbackCaptor [Map [Int, Int]]
    val ltch = Callback.map (0, cb)
    expectResult (Map [Int, Int] ()) (cb.passed)
  }

  it should "reject extra releases" in {
    val cb = new CallbackCaptor [Map [Int, Int]]
    val ltch = Callback.map (0, cb)
    expectResult (Map [Int, Int] ()) (cb.passed)
    intercept [Exception] (ltch (0, 0))
  }

  it should "release after one pass for count==1" in {
    val cb = new CallbackCaptor [Map [Int, Int]]
    val ltch = Callback.map (1, cb)
    assert (!cb.wasInvoked)
    ltch (0, 1)
    expectResult (Map ((0, 1))) (cb.passed)
  }

  it should "release after one fail for count==1" in {
    val cb = new CallbackCaptor [Map [Int, Int]]
    val ltch = Callback.map (1, cb)
    assert (!cb.wasInvoked)
    ltch.fail (new DistinguishedException)
    assert (cb.failed.isInstanceOf [DistinguishedException])
  }

  it should "release after two passes for count==2" in {
    val cb = new CallbackCaptor [Map [Int, Int]]
    val ltch = Callback.map (2, cb)
    assert (!cb.wasInvoked)
    ltch (0, 1)
    assert (!cb.wasInvoked)
    ltch (1, 2)
    expectResult (Map ((0, 1), (1, 2))) (cb.passed)
  }

  it should "release after two reversed passes for count==2" in {
    val cb = new CallbackCaptor [Map [Int, Int]]
    val ltch = Callback.map (2, cb)
    assert (!cb.wasInvoked)
    ltch (1, 2)
    assert (!cb.wasInvoked)
    ltch (0, 1)
    expectResult (Map ((0, 1), (1, 2))) (cb.passed)
  }

  it should "release after a pass and a fail for count==2" in {
    val cb = new CallbackCaptor [Map [Int, Int]]
    val ltch = Callback.map (2, cb)
    assert (!cb.wasInvoked)
    ltch (0, 0)
    assert (!cb.wasInvoked)
    ltch.fail (new DistinguishedException)
    assert (cb.failed.isInstanceOf [DistinguishedException])
  }

  it should "release after two fails for count==2" in {
    val cb = new CallbackCaptor [Map [Int, Int]]
    val ltch = Callback.map (2, cb)
    assert (!cb.wasInvoked)
    ltch.fail (new Exception)
    assert (!cb.wasInvoked)
    ltch.fail (new Exception)
    assert (cb.failed.isInstanceOf [MultiException])
  }}
