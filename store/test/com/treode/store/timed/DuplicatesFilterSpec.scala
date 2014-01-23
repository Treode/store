package com.treode.store.timed

import com.treode.async.{AsyncIterator, Callback, CallbackCaptor}
import com.treode.store.{Bytes, Cardinals, Fruits, TimedCell, TimedTestTools}
import org.scalatest.FlatSpec

import Cardinals.One
import TimedTestTools._

class DuplicatesFilterSpec extends FlatSpec {

  private val Apple = Fruits.Apple ## 1 :: One
  private val Banana = Fruits.Banana ## 1 :: One
  private val Orange = Fruits.Orange ## 1 :: One

  private def expectCells (cs: TimedCell*) (actual: AsyncIterator [TimedCell]) {
    val cb = new CallbackCaptor [Seq [TimedCell]]
    AsyncIterator.scan (actual, cb)
    expectResult (cs) (cb.passed)
  }

  private def newFilter (cs: TimedCell*) = {
    val cb = new CallbackCaptor [AsyncIterator [TimedCell]]
    DuplicatesFilter (AsyncIterator.adapt (cs.iterator), cb)
    cb.passed
  }

  "The DuplicatesFilter" should "handle []" in {
    expectCells () (newFilter ())
  }

  it should "handle [Apple]" in {
    expectCells (Apple) (newFilter (Apple))
  }

  it should "handle [Apple, Apple]" in {
    expectCells (Apple) (newFilter (Apple, Apple))
  }

  it should "handle [Apple, Banana]" in {
    expectCells (Apple, Banana) (newFilter (Apple, Banana))
  }

  it should "handle [Apple, Apple, Banana]" in {
    expectCells (Apple, Banana) (newFilter (Apple, Apple, Banana))
  }

  it should "handle [Apple, Banana, Banana]" in {
    expectCells (Apple, Banana) (newFilter (Apple, Banana, Banana))
  }

  it should "handle [Apple, Banana, Orange]" in {
    expectCells (Apple, Banana, Orange) (newFilter (Apple, Banana, Orange))
  }

  it should "handle [Apple, Apple, Banana, Orange]" in {
    expectCells (Apple, Banana, Orange) (newFilter (Apple, Apple, Banana, Orange))
  }

  it should "handle [Apple, Banana, Banana, Orange]" in {
    expectCells (Apple, Banana, Orange) (newFilter (Apple, Banana, Banana, Orange))
  }

  it should "handle [Apple, Banana, Orange, Orange]" in {
    expectCells (Apple, Banana, Orange) (newFilter (Apple, Banana, Orange, Orange))
  }}
