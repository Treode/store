package com.treode.async

import org.scalatest.FlatSpec

class MergeIteratorSpec extends FlatSpec {

  private def mkIterator [A] (xss: Seq [A] *) (implicit ordering: Ordering [A]) = {
    val cb = new CallbackCaptor [AsyncIterator [A]]
    MergeIterator (xss.iterator map (AsyncIterator.adapt (_)), cb)
    cb.passed
  }

  private def expectSeq [A] (xs: A*) (actual: AsyncIterator [A]) {
    val cb = new CallbackCaptor [Seq [A]]
    AsyncIterator.scan (actual, cb)
    expectResult (xs) (cb.passed)
  }

  "The MergeIterator" should "yield nothing for []" in {
    val iter = mkIterator [Int] ()
    expectSeq () (iter)
  }

  it should "yield nothing for [[]]" in {
    val iter = mkIterator [Int] (Seq.empty)
    expectSeq () (iter)
  }

  it should "yield one thing for [[1]]" in {
    val iter = mkIterator (Seq (1))
    expectSeq (1) (iter)
  }

  it should "yield one thing for [[][1]]" in {
    val iter = mkIterator (Seq.empty, Seq (1))
    expectSeq (1) (iter)
  }

  it should "yield one thing for [[1][]]" in {
    val iter = mkIterator (Seq (1), Seq.empty)
    expectSeq (1) (iter)
  }

  it should "yield two things for [[1, 2]]" in {
    val iter = mkIterator (Seq (1, 2))
    expectSeq (1, 2) (iter)
  }

  it should "yield two things for [[1][2]]" in {
    val iter = mkIterator (Seq (1), Seq (2))
    expectSeq (1, 2) (iter)
  }

  it should "yield two things for [[][1][2]]" in {
    val iter = mkIterator (Seq.empty, Seq (1), Seq (2))
    expectSeq (1, 2) (iter)
  }

  it should "yield two things for [[1][][2]]" in {
    val iter = mkIterator (Seq (1), Seq.empty, Seq (2))
    expectSeq (1, 2) (iter)
  }

  it should "yield two things for [[1][2][]]" in {
    val iter = mkIterator (Seq (1), Seq (2), Seq.empty)
    expectSeq (1, 2) (iter)
  }

  it should "yield two things sorted for [[2][1]]" in {
    val iter = mkIterator (Seq (2), Seq (1))
    expectSeq (1, 2) (iter)
  }

  it should "yield things sorted for [[1][2][3]]" in {
    val iter = mkIterator (Seq (1), Seq (2), Seq (3))
    expectSeq (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[1][3][2]]" in {
    val iter = mkIterator (Seq (1), Seq (3), Seq (2))
    expectSeq (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[2][1][3]]" in {
    val iter = mkIterator (Seq (1), Seq (2), Seq (3))
    expectSeq (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[2][3][1]]" in {
    val iter = mkIterator (Seq (2), Seq (3), Seq (1))
    expectSeq (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[3][1][2]]" in {
    val iter = mkIterator (Seq (3), Seq (1), Seq (2))
    expectSeq (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[3][2][1]]" in {
    val iter = mkIterator (Seq (3), Seq (2), Seq (1))
    expectSeq (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[1, 2][3]]" in {
    val iter = mkIterator (Seq (1, 2), Seq (3))
    expectSeq (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[1][2, 3]]" in {
    val iter = mkIterator (Seq (1), Seq (2, 3))
    expectSeq (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[1, 3][2]]" in {
    val iter = mkIterator (Seq (1, 3), Seq (2))
    expectSeq (1, 2, 3) (iter)
  }

  it should "preserve duplicates in tier order with [[1][1][2]]" in {
    val iter = mkIterator (Seq (1 -> "a"), Seq (1 -> "b"), Seq (2 -> "c"))
    expectSeq (1 -> "a", 1 -> "b", 2 -> "c") (iter)
  }

  it should "preserve duplicates in tier order with [[1][2][1]]" in {
    val iter = mkIterator (Seq (1 -> "a"), Seq (2 -> "b"), Seq (1 -> "c"))
    expectSeq (1 -> "a", 1 -> "c", 2 -> "b") (iter)
  }

  it should "preserve duplicates in teir order with [[2][1][1]]" in {
    val iter = mkIterator (Seq (2 -> "a"), Seq (1 -> "b"), Seq (1 -> "c"))
    expectSeq (1 -> "b", 1 -> "c", 2 -> "a") (iter)
  }}
