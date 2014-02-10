package com.treode.async

import org.scalatest.FlatSpec

class MapLatchSpec extends FlatSpec {

  class DistinguishedException extends Exception

  "The MapLatch" should "release immediately for count==0" in {
    val cb = CallbackCaptor [Map [Int, Int]]
    val ltch = Callback.map (0, cb)
    expectResult (Map [Int, Int] ()) (cb.passed)
  }

  it should "reject extra releases" in {
    val cb = CallbackCaptor [Map [Int, Int]]
    val ltch = Callback.map (0, cb)
    expectResult (Map [Int, Int] ()) (cb.passed)
    intercept [Exception] (ltch (0, 0))
  }

  it should "release after one pass for count==1" in {
    val cb = CallbackCaptor [Map [Int, Int]]
    val ltch = Callback.map (1, cb)
    cb.expectNotInvoked()
    ltch (0, 1)
    expectResult (Map ((0, 1))) (cb.passed)
  }

  it should "release after one fail for count==1" in {
    val cb = CallbackCaptor [Map [Int, Int]]
    val ltch = Callback.map (1, cb)
    cb.expectNotInvoked()
    ltch.fail (new DistinguishedException)
    cb.failed [DistinguishedException]
  }

  it should "release after two passes for count==2" in {
    val cb = CallbackCaptor [Map [Int, Int]]
    val ltch = Callback.map (2, cb)
    cb.expectNotInvoked()
    ltch (0, 1)
    cb.expectNotInvoked()
    ltch (1, 2)
    expectResult (Map ((0, 1), (1, 2))) (cb.passed)
  }

  it should "release after two reversed passes for count==2" in {
    val cb = CallbackCaptor [Map [Int, Int]]
    val ltch = Callback.map (2, cb)
    cb.expectNotInvoked()
    ltch (1, 2)
    cb.expectNotInvoked()
    ltch (0, 1)
    expectResult (Map ((0, 1), (1, 2))) (cb.passed)
  }

  it should "release after a pass and a fail for count==2" in {
    val cb = CallbackCaptor [Map [Int, Int]]
    val ltch = Callback.map (2, cb)
    cb.expectNotInvoked()
    ltch (0, 0)
    cb.expectNotInvoked()
    ltch.fail (new DistinguishedException)
    cb.failed [DistinguishedException]
  }

  it should "release after two fails for count==2" in {
    val cb = CallbackCaptor [Map [Int, Int]]
    val ltch = Callback.map (2, cb)
    cb.expectNotInvoked()
    ltch.fail (new Exception)
    cb.expectNotInvoked()
    ltch.fail (new Exception)
    cb.failed [MultiException]
  }}
