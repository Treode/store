package com.treode.store.local

import com.treode.cluster.concurrent.Callback
import com.treode.store.Fruits
import com.treode.store.tier.{Cell, CellIterator, TestTools}
import org.scalatest.FlatSpec

import Fruits.{Apple, Banana}

class AgeFilterSpec extends FlatSpec with TestTools {

  private def expectCells (cs: Cell*) (actual: CellIterator) =
    expectResult (cs) (actual.toSeq)

  private def newFilter (cs: Cell*) = {
    var iter: CellIterator = null
    AgeFilter (CellIterator.adapt (cs.iterator), 14, new Callback [CellIterator] {
      def apply (_iter: CellIterator) = iter = _iter
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
