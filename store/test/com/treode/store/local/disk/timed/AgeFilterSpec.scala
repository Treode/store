package com.treode.store.local.disk.timed

import com.treode.concurrent.Callback
import com.treode.store.Fruits
import com.treode.store.local.{TimedCell, TimedIterator, TimedTestTools}
import org.scalatest.FlatSpec

import Fruits.{Apple, Banana}
import TimedTestTools._

class AgeFilterSpec extends FlatSpec {

  private def expectCells (cs: TimedCell*) (actual: TimedIterator) =
    expectResult (cs) (actual.toSeq)

  private def newFilter (cs: TimedCell*) = {
    var iter: TimedIterator = null
    AgeFilter (TimedIterator.adapt (cs.iterator), 14, new Callback [TimedIterator] {
      def pass (_iter: TimedIterator) = iter = _iter
      def fail (t: Throwable) = throw t
    })
    assert (iter != null)
    iter
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
