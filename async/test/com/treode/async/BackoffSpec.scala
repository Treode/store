package com.treode.async

import scala.util.Random
import org.scalacheck.Gen
import org.scalatest.FlatSpec
import org.scalatest.prop.PropertyChecks

import PropertyChecks._

class BackoffSpec extends FlatSpec {

  val seeds = Gen.choose (0L, Long.MaxValue)
  val retries = Gen.choose (1, 37)
  val max = Gen.choose (100, 1000)

  "Backoff" should "provide limited retries" in {
    forAll (seeds, retries) { case (seed, retries) =>
      implicit val random = new Random (seed)
      val backoff = Backoff (30, 20, 400, retries)
      assertResult (retries) (backoff.iterator.length)
    }}

  it should "usually grow" in {
    forAll (seeds, max) { case (seed, max) =>
      implicit val random = new Random (seed)
      val backoff = Backoff (30, 20, max, 37)
      var prev = 0
      var count = 0
      for (i <- backoff.iterator) {
        if (i == prev)
          count += 1
        assert (i > prev || i == max || count < 3)
        prev = i
      }}}

  it should "not exceed the maximum" in {
    forAll (seeds, max) { case (seed, max) =>
      implicit val random = new Random (seed)
      val backoff = Backoff (30, 20, max, 37)
      for (i <- backoff.iterator)
        assert (i <= max)
    }}}
