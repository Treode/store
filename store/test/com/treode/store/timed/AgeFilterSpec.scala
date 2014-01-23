package com.treode.store.timed

import com.treode.async.{AsyncIterator, Callback, CallbackCaptor}
import com.treode.store.{Fruits, TimedCell, TimedTestTools}
import org.scalatest.FlatSpec

import Fruits.{Apple, Banana}
import TimedTestTools._

class AgeFilterSpec extends FlatSpec {

  private def expectCells (cs: TimedCell*) (actual: AsyncIterator [TimedCell]) {
    val cb = new CallbackCaptor [Seq [TimedCell]]
    AsyncIterator.scan (actual, cb)
    expectResult (cs) (cb.passed)
  }

  private def newFilter (cs: TimedCell*) = {
    val cb = new CallbackCaptor [AsyncIterator [TimedCell]]
    AgeFilter (AsyncIterator.adapt (cs.iterator), 14, cb)
    cb.passed
  }

  "The AgeFilter" should "handle []" in {
    expectCells () (newFilter ())
  }

  it should "handle [Apple##7]" in {
    expectCells () (newFilter (Apple##7))
  }

  it should "handle [Apple##14]" in {
    expectCells (Apple##14) (newFilter (Apple##14))
  }

  it should "handle [Apple##14, Apple##7]" in {
    expectCells (Apple##14) (newFilter (Apple##14, Apple##7))
  }

  it should "handle [Apple##7, Banana##7]" in {
    expectCells () (newFilter (Apple##7, Banana##7))
  }

  it should "handle [Apple##14, Banana##7]" in {
    expectCells (Apple##14) (newFilter (Apple##14, Banana##7))
  }

  it should "handle [Apple##14, Apple##7, Banana##7]" in {
    expectCells (Apple##14) (newFilter (Apple##14, Apple##7, Banana##7))
  }

  it should "handle [Apple##7, Banana##14]" in {
    expectCells (Banana##14) (newFilter (Apple##7, Banana##14))
  }

  it should "handle [Apple##14, Banana##14]" in {
    expectCells (Apple##14, Banana##14) (newFilter (Apple##14, Banana##14))
  }

  it should "handle [Apple##14, Apple##7, Banana##14]" in {
    expectCells (Apple##14, Banana##14) (newFilter (Apple##14, Apple##7, Banana##14))
  }}
