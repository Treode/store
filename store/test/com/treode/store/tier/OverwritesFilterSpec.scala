package com.treode.store.tier

import com.treode.async.{AsyncConversions, StubScheduler}
import com.treode.store.{Bytes, Fruits}
import org.scalatest.FlatSpec

import AsyncConversions._
import Fruits.{Apple, Banana, Orange}
import TierTestTools._

class OverwritesFilterSpec extends FlatSpec {

  private def expectCells (expected: TierCell*) (actual: TierCellIterator) (implicit s: StubScheduler): Unit =
    assertResult (expected) (actual.toSeq)

  private def newFilter (cs: TierCell*) (implicit s: StubScheduler) =
    OverwritesFilter (cs.iterator.async)

  "The OverwritesFilter" should "handle []" in {
    implicit val scheduler = StubScheduler.random()
    expectCells () (newFilter ())
  }

  it should "handle [Apple::1]" in {
    implicit val scheduler = StubScheduler.random()
    expectCells (Apple::1) (newFilter (Apple::1))
  }

  it should "handle [Apple::2, Apple::1]" in {
    implicit val scheduler = StubScheduler.random()
    expectCells (Apple::2) (newFilter (Apple::2, Apple::1))
  }

  it should "handle [Apple::1, Banana::1]" in {
    implicit val scheduler = StubScheduler.random()
    expectCells (Apple::1, Banana::1) (newFilter (Apple::1, Banana::1))
  }

  it should "handle [Apple::2, Apple::1, Banana::1]" in {
    implicit val scheduler = StubScheduler.random()
    expectCells (Apple::2, Banana::1) (newFilter (Apple::2, Apple::1, Banana::1))
  }

  it should "handle [Apple::1, Banana::2, Banana::2]" in {
    implicit val scheduler = StubScheduler.random()
    expectCells (Apple::1, Banana::2) (newFilter (Apple::1, Banana::2, Banana::1))
  }

  it should "handle [Apple::1, Banana::1, Orange::1]" in {
    implicit val scheduler = StubScheduler.random()
    expectCells (Apple::1, Banana::1, Orange::1) (newFilter (Apple::1, Banana::1, Orange::1))
  }

  it should "handle [Apple::2, Apple::1, Banana::1, Orange::1]" in {
    implicit val scheduler = StubScheduler.random()
    expectCells (Apple::2, Banana::1, Orange::1) (
        newFilter (Apple::2, Apple::1, Banana::1, Orange::1))
  }

  it should "handle [Apple::1, Banana, Banana::1, Orange::1]" in {
    implicit val scheduler = StubScheduler.random()
    expectCells (Apple::1, Banana::2, Orange::1) (
        newFilter (Apple::1, Banana::2, Banana::1, Orange::1))
  }

  it should "handle [Apple::1, Banana::1, Orange, Orange::1]" in {
    implicit val scheduler = StubScheduler.random()
    expectCells (Apple::1, Banana::1, Orange::2) (
        newFilter (Apple::1, Banana::1, Orange::2, Orange::1))
  }}
