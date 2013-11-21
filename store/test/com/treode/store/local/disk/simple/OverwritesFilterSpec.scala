package com.treode.store.local.disk.simple

import com.treode.async.Callback
import com.treode.store.{Bytes, Fruits, SimpleCell}
import com.treode.store.local.{SimpleIterator, LocalSimpleTestTools}
import org.scalatest.FlatSpec

import Fruits.{Apple, Banana, Orange}
import LocalSimpleTestTools._

class OverwritesFilterSpec extends FlatSpec {

  private def expectCells (cs: SimpleCell*) (actual: SimpleIterator) =
    expectResult (cs) (actual.toSeq)

  private def newFilter (cs: SimpleCell*) = {
    var iter: SimpleIterator = null
    OverwritesFilter (SimpleIterator.adapt (cs.iterator), new Callback [SimpleIterator] {
      def pass (_iter: SimpleIterator) = iter = _iter
      def fail (t: Throwable) = throw t
    })
    assert (iter != null)
    iter
  }

  "The OverwritesFilter" should "handle []" in {
    expectCells () (newFilter ())
  }

  it should "handle [Apple::1]" in {
    expectCells (Apple::1) (newFilter (Apple::1))
  }

  it should "handle [Apple::2, Apple::1]" in {
    expectCells (Apple::2) (newFilter (Apple::2, Apple::1))
  }

  it should "handle [Apple::1, Banana::1]" in {
    expectCells (Apple::1, Banana::1) (newFilter (Apple::1, Banana::1))
  }

  it should "handle [Apple::2, Apple::1, Banana::1]" in {
    expectCells (Apple::2, Banana::1) (newFilter (Apple::2, Apple::1, Banana::1))
  }

  it should "handle [Apple::1, Banana::2, Banana::2]" in {
    expectCells (Apple::1, Banana::2) (newFilter (Apple::1, Banana::2, Banana::1))
  }

  it should "handle [Apple::1, Banana::1, Orange::1]" in {
    expectCells (Apple::1, Banana::1, Orange::1) (newFilter (Apple::1, Banana::1, Orange::1))
  }

  it should "handle [Apple::2, Apple::1, Banana::1, Orange::1]" in {
    expectCells (Apple::2, Banana::1, Orange::1) (
        newFilter (Apple::2, Apple::1, Banana::1, Orange::1))
  }

  it should "handle [Apple::1, Banana, Banana::1, Orange::1]" in {
    expectCells (Apple::1, Banana::2, Orange::1) (
        newFilter (Apple::1, Banana::2, Banana::1, Orange::1))
  }

  it should "handle [Apple::1, Banana::1, Orange, Orange::1]" in {
    expectCells (Apple::1, Banana::1, Orange::2) (
        newFilter (Apple::1, Banana::1, Orange::2, Orange::1))
  }}
