package com.treode.async

import com.treode.async.stubs.{AsyncTestTools, StubScheduler}
import org.scalatest.FlatSpec

import Async.supply
import AsyncTestTools._
import Callback.{ignore => disregard}

class RichExecutorSpec extends FlatSpec {

  class DistinguishedException extends Exception

  "Async.whilst" should "handle zero iterations" in {
    implicit val s = StubScheduler.random()
    var count = 0
    s.whilst (false) (supply (count += 1)) .pass
    assertResult (0) (count)
  }

  it should "handle one iteration" in {
    implicit val s = StubScheduler.random()
    var count = 0
    s.whilst (count < 1) (supply (count += 1)) .pass
    assertResult (1) (count)
  }

  it should "handle multiple iterations" in {
    implicit val s = StubScheduler.random()
    var count = 0
    s.whilst (count < 3) (supply (count += 1)) .pass
    assertResult (3) (count)
  }

  it should "pass an exception from the condition to the callback" in {
    implicit val s = StubScheduler.random()
    var count = 0
    s.whilst {
      if (count == 3)
        throw new DistinguishedException
      true
    } (supply (count += 1)) .fail [DistinguishedException]
    assertResult (3) (count)
  }

    it should "pass an exception returned from the body to the callback" in {
    implicit val s = StubScheduler.random()
    var count = 0
    s.whilst (true) {
      supply {
        count += 1
        if (count == 3)
          throw new DistinguishedException
      }
    } .fail [DistinguishedException]
    assertResult (3) (count)
  }

  it should "pass an exception thrown from the body to the callback" in {
    implicit val s = StubScheduler.random()
    var count = 0
    s.whilst (true) {
      if (count == 3)
        throw new DistinguishedException
      supply (count += 1)
    } .fail [DistinguishedException]
    assertResult (3) (count)
  }}
