package com.treode.store.local.disk.timed

import com.treode.concurrent.Callback
import com.treode.store.{Bytes, Fruits}
import com.treode.store.local.{TimedCell, TimedIterator, TimedTestTools}
import org.scalatest.FlatSpec

import TimedTestTools._

class DuplicatesFilterSpec extends FlatSpec {

  private val One = Bytes ("one")

  private val Apple = Fruits.Apple ## 1 :: 1
  private val Banana = Fruits.Banana ## 1 :: 1
  private val Orange = Fruits.Orange ## 1 :: 1

  private def expectCells (cs: TimedCell*) (actual: TimedIterator) =
    expectResult (cs) (actual.toSeq)

  private def newFilter (cs: TimedCell*) = {
    var iter: TimedIterator = null
    DuplicatesFilter (TimedIterator.adapt (cs.iterator), new Callback [TimedIterator] {
      def pass (_iter: TimedIterator) = iter = _iter
      def fail (t: Throwable) = throw t
    })
    assert (iter != null)
    iter
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
