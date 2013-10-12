package com.treode.store.local

import com.treode.cluster.concurrent.Callback
import com.treode.store.{Bytes, Fruits}
import com.treode.store.tier.{Cell, CellIterator, TestTools}
import org.scalatest.FlatSpec

import Fruits.{Apple, Banana}

class DeletesFilterSpec extends FlatSpec with TestTools {

  private val One = Bytes ("one")

  private def expectCells (cs: Cell*) (actual: CellIterator) =
    expectResult (cs) (actual.toSeq)

  private def newFilter (cs: Cell*) = {
    var iter: CellIterator = null
    DeletesFilter (CellIterator.adapt (cs.iterator), new Callback [CellIterator] {
      def apply (_iter: CellIterator) = iter = _iter
      def fail (t: Throwable) = throw t
    })
    assert (iter != null)
    iter
  }

  "The DeletesFilter" should "handle []" in {
    expectCells () (newFilter ())
  }

  it should "handle [Apple##2]" in {
    expectCells () (newFilter (Apple##2))
  }

  it should "handle [Apple##2::One]" in {
    expectCells (Apple##2::One) (newFilter (Apple##2::One))
  }

  it should "handle [Apple##2::One, Apple##1]" in {
    expectCells (Apple##2::One) (newFilter (Apple##2::One, Apple##1))
  }

  it should "handle [Apple##2, Apple##1::One]" in {
    expectCells (Apple##2, Apple##1::One) (newFilter (Apple##2, Apple##1::One))
  }

  it should "handle [Apple##2::One, Apple##1::One]" in {
    expectCells (Apple##2::One, Apple##1::One) (newFilter (Apple##2::One, Apple##1::One))
  }

  it should "handle [Apple##2, Banana##2]" in {
    expectCells () (newFilter (Apple##2, Banana##2))
  }

  it should "handle [Apple##2::One, Banana##2]" in {
    expectCells (Apple##2::One) (newFilter (Apple##2::One, Banana##2))
  }

  it should "handle [Apple##2::One, Apple##1, Banana##2]" in {
    expectCells (Apple##2::One) (newFilter (Apple##2::One, Apple##1, Banana##2))
  }

  it should "handle [Apple##2, Apple##1::One, Banana##2]" in {
    expectCells (Apple##2, Apple##1::One) (newFilter (Apple##2, Apple##1::One, Banana##2))
  }

  it should "handle [Apple##2::One, Apple##1::One, Banana##2]" in {
    expectCells (Apple##2::One, Apple##1::One) (
        newFilter (Apple##2::One, Apple##1::One, Banana##2))
  }

  it should "handle [Apple##2, Banana##2::One]" in {
    expectCells (Banana##2::One) (newFilter (Apple##2, Banana##2::One))
  }

  it should "handle [Apple##2::One, Banana##2::One]" in {
    expectCells (Apple##2::One, Banana##2::One) (newFilter (Apple##2::One, Banana##2::One))
  }

  it should "handle [Apple##2::One, Apple##1, Banana##2::One]" in {
    expectCells (Apple##2::One, Banana##2::One) (
        newFilter (Apple##2::One, Apple##1, Banana##2::One))
  }

  it should "handle [Apple##2, Apple##1::One, Banana##2::One]" in {
    expectCells (Apple##2, Apple##1::One, Banana##2::One) (
        newFilter (Apple##2, Apple##1::One, Banana##2::One))
  }

  it should "handle [Apple##2::One, Apple##1::One, Banana##2::One]" in {
    expectCells (Apple##2::One, Apple##1::One, Banana##2::One) (
        newFilter (Apple##2::One, Apple##1::One, Banana##2::One))
  }

  it should "handle [Apple##2, Banana##2::One, Banana##1]" in {
    expectCells (Banana##2::One) (newFilter (Apple##2, Banana##2::One, Banana##1))
  }

  it should "handle [Apple##2::One, Banana##2::One, Banana##1]" in {
    expectCells (Apple##2::One, Banana##2::One) (
        newFilter (Apple##2::One, Banana##2::One, Banana##1))
  }

  it should "handle [Apple##2::One, Apple##1, Banana##2::One, Banana##1]" in {
    expectCells (Apple##2::One, Banana##2::One) (
        newFilter (Apple##2::One, Apple##1, Banana##2::One, Banana##1))
  }

  it should "handle [Apple##2, Apple##1::One, Banana##2::One, Banana##1]" in {
    expectCells (Apple##2, Apple##1::One, Banana##2::One) (
        newFilter (Apple##2, Apple##1::One, Banana##2::One, Banana##1))
  }

  it should "handle [Apple##2::One, Apple##1::One, Banana##2::One, Banana##1]" in {
    expectCells (Apple##2::One, Apple##1::One, Banana##2::One) (
        newFilter (Apple##2::One, Apple##1::One, Banana##2::One, Banana##1))
  }}
