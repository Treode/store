package com.treode.store.local.disk.simple

import com.treode.concurrent.Callback
import com.treode.store.{Bytes, Fruits}
import com.treode.store.local.{SimpleCell, SimpleIterator, SimpleTestTools}
import org.scalatest.FlatSpec

import SimpleTestTools._

class SimpleSynthIteratorSpec extends FlatSpec {

  private val Apple = Fruits.Apple::1
  private val Apple2 = Fruits.Apple::2
  private val Banana = Fruits.Banana::1
  private val Orange = Fruits.Orange::1

  private implicit class RichSynthIterator (iter: SynthIterator) {

    def add (cells: SimpleCell*) {
      iter.enqueue (Iterator (SimpleIterator.adapt (cells.iterator)), Callback.ignore)
    }}

  private def expectCells (cs: SimpleCell*) (actual: SimpleIterator) =
    expectResult (cs) (actual.toSeq)

  "The simple SynthIterator" should "yield nothing for []" in {
    val iter = new SynthIterator
    expectCells () (iter)
  }

  it should "yield nothing for [[]]" in {
    val iter = new SynthIterator
    iter.add()
    expectCells () (iter)
  }

  it should "yield one thing for [[Apple]]" in {
    val iter = new SynthIterator
    iter.add (Apple)
    expectCells (Apple) (iter)
  }

  it should "yield one thing for [[][Apple]]" in {
    val iter = new SynthIterator
    iter.add()
    iter.add (Apple)
    expectCells (Apple) (iter)
  }

  it should "yield one thing for [[Apple][]]" in {
    val iter = new SynthIterator
    iter.add (Apple)
    iter.add()
    expectCells (Apple) (iter)
  }

  it should "yield two things for [[Apple, Banana]]" in {
    val iter = new SynthIterator
    iter.add (Apple, Banana)
    expectCells (Apple, Banana) (iter)
  }

  it should "yield two things for [[Apple][Banana]]" in {
    val iter = new SynthIterator
    iter.add (Apple)
    iter.add (Banana)
    expectCells (Apple, Banana) (iter)
  }

  it should "yield two things for [[][Apple][Banana]]" in {
    val iter = new SynthIterator
    iter.add()
    iter.add (Apple)
    iter.add (Banana)
    expectCells (Apple, Banana) (iter)
  }

  it should "yield two things for [[Apple][][Banana]]" in {
    val iter = new SynthIterator
    iter.add (Apple)
    iter.add()
    iter.add (Banana)
    expectCells (Apple, Banana) (iter)
  }

  it should "yield two things for [[Apple][Banana][]]" in {
    val iter = new SynthIterator
    iter.add (Apple)
    iter.add (Banana)
    iter.add()
    expectCells (Apple, Banana) (iter)
  }

  it should "yield two things sorted for [[Banana][Apple]]" in {
    val iter = new SynthIterator
    iter.add (Banana)
    iter.add (Apple)
    expectCells (Apple, Banana) (iter)
  }

  it should "preserve duplicates in tier order with [[Apple][Apple2]]" in {
    val iter = new SynthIterator
    iter.add (Apple)
    iter.add (Apple2)
    expectCells (Apple, Apple2) (iter)
  }

  it should "preserve duplicates in tier order with [[Apple2][Apple]]" in {
    val iter = new SynthIterator
    iter.add (Apple2)
    iter.add (Apple)
    expectCells (Apple2, Apple) (iter)
  }

  it should "yield things sorted for [[Apple][Banana][Orange]]" in {
    val iter = new SynthIterator
    iter.add (Apple)
    iter.add (Banana)
    iter.add (Orange)
    expectCells (Apple, Banana, Orange) (iter)
  }

  it should "yield things sorted for [[Apple][Orange][Banana]]" in {
    val iter = new SynthIterator
    iter.add (Apple)
    iter.add (Orange)
    iter.add (Banana)
    expectCells (Apple, Banana, Orange) (iter)
  }

  it should "yield things sorted for [[Banana][Apple][Orange]]" in {
    val iter = new SynthIterator
    iter.add (Banana)
    iter.add (Apple)
    iter.add (Orange)
    expectCells (Apple, Banana, Orange) (iter)
  }

  it should "yield things sorted for [[Banana][Orange][Apple]]" in {
    val iter = new SynthIterator
    iter.add (Banana)
    iter.add (Orange)
    iter.add (Apple)
    expectCells (Apple, Banana, Orange) (iter)
  }

  it should "yield things sorted for [[Orange][Apple][Banana]]" in {
    val iter = new SynthIterator
    iter.add (Orange)
    iter.add (Apple)
    iter.add (Banana)
    expectCells (Apple, Banana, Orange) (iter)
  }

  it should "yield things sorted for [[Orange][Banana][Apple]]" in {
    val iter = new SynthIterator
    iter.add (Orange)
    iter.add (Banana)
    iter.add (Apple)
    expectCells (Apple, Banana, Orange) (iter)
  }

  it should "yield things sorted for [[Apple, Banana][Orange]]" in {
    val iter = new SynthIterator
    iter.add (Apple, Banana)
    iter.add (Orange)
    expectCells (Apple, Banana, Orange) (iter)
  }

  it should "yield things sorted for [[Apple][Banana, Orange]]" in {
    val iter = new SynthIterator
    iter.add (Apple)
    iter.add (Banana, Orange)
    expectCells (Apple, Banana, Orange) (iter)
  }

  it should "yield things sorted for [[Apple, Orange][Banana]]" in {
    val iter = new SynthIterator
    iter.add (Apple, Orange)
    iter.add (Banana)
    expectCells (Apple, Banana, Orange) (iter)
  }

  it should "preserve duplicates with [[Apple][Apple][Banana]]" in {
    val iter = new SynthIterator
    iter.add (Apple)
    iter.add (Apple)
    iter.add (Banana)
    expectCells (Apple, Apple, Banana) (iter)
  }

  it should "preserve duplicates with [[Apple][Banana][Apple]]" in {
    val iter = new SynthIterator
    iter.add (Apple)
    iter.add (Banana)
    iter.add (Apple)
    expectCells (Apple, Apple, Banana) (iter)
  }

  it should "preserve duplicates with [[Banana][Apple][Apple]]" in {
    val iter = new SynthIterator
    iter.add (Banana)
    iter.add (Apple)
    iter.add (Apple)
    expectCells (Apple, Apple, Banana) (iter)
  }}
