package com.treode.async

import org.scalatest.FlatSpec

class MergeIteratorSpec extends FlatSpec {

  private implicit class RichSynthIterator [A] (iter: MergeIterator [A]) {

    def adds (xss: Seq [A] *) {
       val cb = new CallbackCaptor [Unit]
      iter.enqueue (xss.iterator map (AsyncIterator.adapt (_)), cb)
      cb.passed
    }

    def add (xs: A*): Unit =
      adds (xs)
  }

  private def expectCells [A] (xs: A*) (actual: AsyncIterator [A]) {
    val cb = new CallbackCaptor [Seq [A]]
    AsyncIterator.scan (actual, cb)
    expectResult (xs) (cb.passed)
  }

  "The MergeIterator" should "yield nothing for []" in {
    val iter = new MergeIterator [Int]
    expectCells () (iter)
  }

  it should "yield nothing for [[]]" in {
    val iter = new MergeIterator [Int]
    iter.add()
    expectCells () (iter)
  }

  it should "yield one thing for [[1]]" in {
    val iter = new MergeIterator [Int]
    iter.add (1)
    expectCells (1) (iter)
  }

  it should "yield one thing for [[][1]]" in {
    val iter = new MergeIterator [Int]
    iter.add()
    iter.add (1)
    expectCells (1) (iter)
  }

  it should "yield one thing for [[][1]] (2)" in {
    val iter = new MergeIterator [Int]
    iter.adds (Seq.empty, Seq (1))
    expectCells (1) (iter)
  }

  it should "yield one thing for [[1][]]" in {
    val iter = new MergeIterator [Int]
    iter.add (1)
    iter.add()
    expectCells (1) (iter)
  }

  it should "yield one thing for [[1][]] (2)" in {
    val iter = new MergeIterator [Int]
    iter.adds (Seq (1), Seq.empty)
    expectCells (1) (iter)
  }

  it should "yield two things for [[1, 2]]" in {
    val iter = new MergeIterator [Int]
    iter.add (1, 2)
    expectCells (1, 2) (iter)
  }

  it should "yield two things for [[1][2]]" in {
    val iter = new MergeIterator [Int]
    iter.add (1)
    iter.add (2)
    expectCells (1, 2) (iter)
  }

  it should "yield two things for [[1][2]] (2)" in {
    val iter = new MergeIterator [Int]
    iter.adds (Seq (1), Seq (2))
    expectCells (1, 2) (iter)
  }

  it should "yield two things for [[][1][2]]" in {
    val iter = new MergeIterator [Int]
    iter.add()
    iter.add (1)
    iter.add (2)
    expectCells (1, 2) (iter)
  }

  it should "yield two things for [[][1][2]] (2)" in {
    val iter = new MergeIterator [Int]
    iter.adds (Seq.empty, Seq (1), Seq (2))
    expectCells (1, 2) (iter)
  }

  it should "yield two things for [[1][][2]]" in {
    val iter = new MergeIterator [Int]
    iter.add (1)
    iter.add()
    iter.add (2)
    expectCells (1, 2) (iter)
  }

  it should "yield two things for [[1][][2]] (2)" in {
    val iter = new MergeIterator [Int]
    iter.adds (Seq (1), Seq.empty, Seq (2))
    expectCells (1, 2) (iter)
  }

  it should "yield two things for [[1][2][]]" in {
    val iter = new MergeIterator [Int]
    iter.add (1)
    iter.add (2)
    iter.add()
    expectCells (1, 2) (iter)
  }

  it should "yield two things for [[1][2][]] (2)" in {
    val iter = new MergeIterator [Int]
    iter.adds (Seq (1), Seq (2), Seq.empty)
    expectCells (1, 2) (iter)
  }

  it should "yield two things sorted for [[2][1]]" in {
    val iter = new MergeIterator [Int]
    iter.add (2)
    iter.add (1)
    expectCells (1, 2) (iter)
  }

  it should "yield two things sorted for [[2][1]] (2)" in {
    val iter = new MergeIterator [Int]
    iter.adds (Seq (2), Seq (1))
    expectCells (1, 2) (iter)
  }

  it should "yield things sorted for [[1][2][3]]" in {
    val iter = new MergeIterator [Int]
    iter.add (1)
    iter.add (2)
    iter.add (3)
    expectCells (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[1][2][3]] (2)" in {
    val iter = new MergeIterator [Int]
    iter.adds (Seq (1), Seq (2), Seq (3))
    expectCells (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[1][3][2]]" in {
    val iter = new MergeIterator [Int]
    iter.add (1)
    iter.add (3)
    iter.add (2)
    expectCells (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[1][3][2]] (2)" in {
    val iter = new MergeIterator [Int]
    iter.adds (Seq (1), Seq (3), Seq (2))
    expectCells (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[2][1][3]]" in {
    val iter = new MergeIterator [Int]
    iter.add (2)
    iter.add (1)
    iter.add (3)
    expectCells (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[2][1][3]] (2)" in {
    val iter = new MergeIterator [Int]
    iter.adds (Seq (1), Seq (2), Seq (3))
    expectCells (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[2][3][1]]" in {
    val iter = new MergeIterator [Int]
    iter.add (2)
    iter.add (3)
    iter.add (1)
    expectCells (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[2][3][1]] (2)" in {
    val iter = new MergeIterator [Int]
    iter.adds (Seq (2), Seq (3), Seq (1))
    expectCells (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[3][1][2]]" in {
    val iter = new MergeIterator [Int]
    iter.add (3)
    iter.add (1)
    iter.add (2)
    expectCells (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[3][1][2]] (2)" in {
    val iter = new MergeIterator [Int]
    iter.adds (Seq (3), Seq (1), Seq (2))
    expectCells (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[3][2][1]]" in {
    val iter = new MergeIterator [Int]
    iter.add (3)
    iter.add (2)
    iter.add (1)
    expectCells (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[3][2][1]] (2)" in {
    val iter = new MergeIterator [Int]
    iter.adds (Seq (3), Seq (2), Seq (1))
    expectCells (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[1, 2][3]]" in {
    val iter = new MergeIterator [Int]
    iter.add (1, 2)
    iter.add (3)
    expectCells (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[1, 2][3]] (2)" in {
    val iter = new MergeIterator [Int]
    iter.adds (Seq (1, 2), Seq (3))
    expectCells (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[1][2, 3]]" in {
    val iter = new MergeIterator [Int]
    iter.add (1)
    iter.add (2, 3)
    expectCells (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[1][2, 3]] (2)" in {
    val iter = new MergeIterator [Int]
    iter.adds (Seq (1), Seq (2, 3))
    expectCells (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[1, 3][2]]" in {
    val iter = new MergeIterator [Int]
    iter.add (1, 3)
    iter.add (2)
    expectCells (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[1, 3][2]] (2)" in {
    val iter = new MergeIterator [Int]
    iter.adds (Seq (1, 3), Seq (2))
    expectCells (1, 2, 3) (iter)
  }

  it should "preserve duplicates in tier order with [[1][1][2]]" in {
    val iter = new MergeIterator [(Int, String)] () (Ordering.by (_._1))
    iter.adds (Seq (1 -> "a"), Seq (1 -> "b"), Seq (2 -> "c"))
    expectCells (1 -> "a", 1 -> "b", 2 -> "c") (iter)
  }

  it should "preserve duplicates in tier order with [[1][2][1]]" in {
    val iter = new MergeIterator [(Int, String)] () (Ordering.by (_._1))
    iter.adds (Seq (1 -> "a"), Seq (2 -> "b"), Seq (1 -> "c"))
    expectCells (1 -> "a", 1 -> "c", 2 -> "b") (iter)
  }

  it should "preserve duplicates in teir order with [[2][1][1]]" in {
    val iter = new MergeIterator [(Int, String)] () (Ordering.by (_._1))
    iter.adds (Seq (2 -> "a"), Seq (1 -> "b"), Seq (1 -> "c"))
    expectCells (1 -> "b", 1 -> "c", 2 -> "a") (iter)
  }}
