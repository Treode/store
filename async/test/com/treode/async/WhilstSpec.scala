package com.treode.async

import org.scalatest.FlatSpec

import AsyncTestTools._
import Callback.{ignore => disregard}

class WhilstSpec extends FlatSpec {

  class DistinguishedException extends Exception

  def whilst (implicit s: Scheduler) = new Whilst (s)

  "Async.whilst" should "handle zero iterations" in {
    implicit val scheduler = StubScheduler.random()
    var count = 0
    whilst.f (false) (count += 1) .pass
    expectResult (0) (count)
  }

  it should "handle one iteration" in {
    implicit val scheduler = StubScheduler.random()
    var count = 0
    whilst.f (count < 1) (count += 1) .pass
    expectResult (1) (count)
  }

  it should "handle multiple iterations" in {
    implicit val scheduler = StubScheduler.random()
    var count = 0
    whilst.f (count < 3) (count += 1) .pass
    expectResult (3) (count)
  }

  it should "handle pass an exception from the body to the callback" in {
    implicit val scheduler = StubScheduler.random()
    var count = 0
    whilst.f (true) {
      count += 1
      if (count == 3)
        throw new DistinguishedException
    } .fail [DistinguishedException]
    expectResult (3) (count)
  }

  it should "handle pass an exception from the conditions to the callback" in {
    implicit val scheduler = StubScheduler.random()
    var count = 0
    whilst.f {
      if (count == 3)
        throw new DistinguishedException
      true
    } (count += 1) .fail [DistinguishedException]
    expectResult (3) (count)
  }}
