package com.treode.async

import org.scalatest.FlatSpec

import com.treode.async.implicits._

class CountingLatchSpec extends FlatSpec {

  class DistinguishedException extends Exception

  "The CountingLatch" should "release immediately for count==0" in {
    val cb = CallbackCaptor [Unit]
    val ltch = Latch.unit (0, cb)
    cb.passed
  }

  it should "reject extra releases" in {
    val cb = CallbackCaptor [Unit]
    val ltch = Latch.unit [Int] (0, cb)
    cb.passed
    intercept [Exception] (ltch.pass (0))
  }

  it should "release after one pass for count==1" in {
    val cb = CallbackCaptor [Unit]
    val ltch = Latch.unit [Int] (1, cb)
    cb.assertNotInvoked()
    ltch.pass (0)
    cb.passed
  }

  it should "release after one fail for count==1" in {
    val cb = CallbackCaptor [Unit]
    val ltch = Latch.unit [Int] (1, cb)
    cb.assertNotInvoked()
    ltch.fail (new DistinguishedException)
    cb.failed [DistinguishedException]
  }

  it should "release after two passes for count==2" in {
    val cb = CallbackCaptor [Unit]
    val ltch = Latch.unit [Int] (2, cb)
    cb.assertNotInvoked()
    ltch.pass (0)
    cb.assertNotInvoked()
    ltch.pass (0)
    cb.passed
  }

  it should "release after a pass and a fail for count==2" in {
    val cb = CallbackCaptor [Unit]
    val ltch = Latch.unit [Int] (2, cb)
    cb.assertNotInvoked()
    ltch.pass (0)
    cb.assertNotInvoked()
    ltch.fail (new DistinguishedException)
    cb.failed [DistinguishedException]
  }

  it should "release after two fails for count==2" in {
    val cb = CallbackCaptor [Unit]
    val ltch = Latch.unit [Int] (2, cb)
    cb.assertNotInvoked()
    ltch.fail (new Exception)
    cb.assertNotInvoked()
    ltch.fail (new Exception)
    cb.failed [MultiException]
  }}
