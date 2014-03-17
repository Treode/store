package com.treode.async

import org.scalatest.FlatSpec

import AsyncConversions._

class MapLatchSpec extends FlatSpec {

  class DistinguishedException extends Exception

  "The MapLatch" should "release immediately for count==0" in {
    val cb = CallbackCaptor [Map [Int, Int]]
    val ltch = Latch.map [Int, Int] (0, cb)
    assertResult (Map [Int, Int] ()) (cb.passed)
  }

  it should "reject extra releases" in {
    val cb = CallbackCaptor [Map [Int, Int]]
    val ltch = Latch.map [Int, Int] (0, cb)
    assertResult (Map [Int, Int] ()) (cb.passed)
    intercept [Exception] (ltch.pass (0, 0))
  }

  it should "release after one pass for count==1" in {
    val cb = CallbackCaptor [Map [Int, Int]]
    val ltch = Latch.map [Int, Int] (1, cb)
    cb.assertNotInvoked()
    ltch.pass (0, 1)
    assertResult (Map ((0, 1))) (cb.passed)
  }

  it should "release after one fail for count==1" in {
    val cb = CallbackCaptor [Map [Int, Int]]
    val ltch = Latch.map [Int, Int] (1, cb)
    cb.assertNotInvoked()
    ltch.fail (new DistinguishedException)
    cb.failed [DistinguishedException]
  }

  it should "release after two passes for count==2" in {
    val cb = CallbackCaptor [Map [Int, Int]]
    val ltch = Latch.map [Int, Int] (2, cb)
    cb.assertNotInvoked()
    ltch.pass (0, 1)
    cb.assertNotInvoked()
    ltch.pass (1, 2)
    assertResult (Map ((0, 1), (1, 2))) (cb.passed)
  }

  it should "release after two reversed passes for count==2" in {
    val cb = CallbackCaptor [Map [Int, Int]]
    val ltch = Latch.map [Int, Int] (2, cb)
    cb.assertNotInvoked()
    ltch.pass (1, 2)
    cb.assertNotInvoked()
    ltch.pass (0, 1)
    assertResult (Map ((0, 1), (1, 2))) (cb.passed)
  }

  it should "release after a pass and a fail for count==2" in {
    val cb = CallbackCaptor [Map [Int, Int]]
    val ltch = Latch.map [Int, Int] (2, cb)
    cb.assertNotInvoked()
    ltch.pass (0, 0)
    cb.assertNotInvoked()
    ltch.fail (new DistinguishedException)
    cb.failed [DistinguishedException]
  }

  it should "release after two fails for count==2" in {
    val cb = CallbackCaptor [Map [Int, Int]]
    val ltch = Latch.map [Int, Int] (2, cb)
    cb.assertNotInvoked()
    ltch.fail (new Exception)
    cb.assertNotInvoked()
    ltch.fail (new Exception)
    cb.failed [MultiException]
  }}
