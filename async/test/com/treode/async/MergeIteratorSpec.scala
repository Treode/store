package com.treode.async

import org.scalatest.FlatSpec

import AsyncIteratorTestTools._

class MergeIteratorSpec extends FlatSpec {

  private def merge [A] (xss: Seq [A] *) (implicit s: StubScheduler, ord: Ordering [A]) =
    AsyncIterator.merge (xss map (_.async))

  "The MergeIterator" should "yield nothing for []" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge [Int] ()
    expectSeq () (iter)
  }

  it should "yield nothing for [[]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge [Int] (Seq.empty)
    expectSeq () (iter)
  }

  it should "yield one thing for [[1]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq (1))
    expectSeq (1) (iter)
  }

  it should "yield one thing for [[][1]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq.empty, Seq (1))
    expectSeq (1) (iter)
  }

  it should "yield one thing for [[1][]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq (1), Seq.empty)
    expectSeq (1) (iter)
  }

  it should "yield two things for [[1, 2]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq (1, 2))
    expectSeq (1, 2) (iter)
  }

  it should "yield two things for [[1][2]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq (1), Seq (2))
    expectSeq (1, 2) (iter)
  }

  it should "yield two things for [[][1][2]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq.empty, Seq (1), Seq (2))
    expectSeq (1, 2) (iter)
  }

  it should "yield two things for [[1][][2]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq (1), Seq.empty, Seq (2))
    expectSeq (1, 2) (iter)
  }

  it should "yield two things for [[1][2][]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq (1), Seq (2), Seq.empty)
    expectSeq (1, 2) (iter)
  }

  it should "yield two things sorted for [[2][1]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq (2), Seq (1))
    expectSeq (1, 2) (iter)
  }

  it should "yield things sorted for [[1][2][3]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq (1), Seq (2), Seq (3))
    expectSeq (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[1][3][2]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq (1), Seq (3), Seq (2))
    expectSeq (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[2][1][3]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq (1), Seq (2), Seq (3))
    expectSeq (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[2][3][1]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq (2), Seq (3), Seq (1))
    expectSeq (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[3][1][2]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq (3), Seq (1), Seq (2))
    expectSeq (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[3][2][1]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq (3), Seq (2), Seq (1))
    expectSeq (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[1, 2][3]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq (1, 2), Seq (3))
    expectSeq (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[1][2, 3]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq (1), Seq (2, 3))
    expectSeq (1, 2, 3) (iter)
  }

  it should "yield things sorted for [[1, 3][2]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq (1, 3), Seq (2))
    expectSeq (1, 2, 3) (iter)
  }

  it should "preserve duplicates in tier order with [[1][1][2]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq (1 -> "a"), Seq (1 -> "b"), Seq (2 -> "c"))
    expectSeq (1 -> "a", 1 -> "b", 2 -> "c") (iter)
  }

  it should "preserve duplicates in tier order with [[1][2][1]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq (1 -> "a"), Seq (2 -> "b"), Seq (1 -> "c"))
    expectSeq (1 -> "a", 1 -> "c", 2 -> "b") (iter)
  }

  it should "preserve duplicates in tier order with [[2][1][1]]" in {
    implicit val scheduler = StubScheduler.random()
    val iter = merge (Seq (2 -> "a"), Seq (1 -> "b"), Seq (1 -> "c"))
    expectSeq (1 -> "b", 1 -> "c", 2 -> "a") (iter)
  }

  it should "stop at an exception in the first iterator" in {
    implicit val scheduler = StubScheduler.random()
    var c1 = Set.empty [Int]
    var c2 = Set.empty [Int]
    var provided = Set.empty [Int]
    expectFail [DistinguishedException] {
      val i1 = track (adapt (1, 3, 5, 7)) (c1 += _)
      val i2 = failWhen (i1) (_ == 5)
      val j1 = track (adapt (2, 4, 6, 8)) (c2 += _)
      track (AsyncIterator.merge (Seq (i2, j1))) (provided += _)
    }
    expectResult (Set (1, 3, 5)) (c1)
    expectResult (Set (2, 4)) (c2)
    expectResult (Set (1, 2, 3)) (provided)
  }

  it should "stop at an exception in the second iterator" in {
    implicit val scheduler = StubScheduler.random()
    var c1 = Set.empty [Int]
    var c2 = Set.empty [Int]
    var provided = Set.empty [Int]
    expectFail [DistinguishedException] {
      val i1 = track (adapt (1, 3, 5, 7)) (c1 += _)
      val j1 = track (adapt (2, 4, 6, 8)) (c2 += _)
      val j2 = failWhen (j1) (_ == 6)
      track (AsyncIterator.merge (Seq (i1, j2))) (provided += _)
    }
    expectResult (Set (1, 3, 5)) (c1)
    expectResult (Set (2, 4, 6)) (c2)
    expectResult (Set (1, 2, 3, 4)) (provided)
  }

  it should "get exceptions from both iterators" in {
    implicit val scheduler = StubScheduler.random()
    var c1 = Set.empty [Int]
    var c2 = Set.empty [Int]
    var provided = Set.empty [Int]
    expectFail [MultiException] {
      val i1 = track (adapt (1, 3, 5, 7)) (c1 += _)
      val i2 = failWhen (i1) (_ => true)
      val j1 = track (adapt (2, 4, 6, 8)) (c2 += _)
      val j2 = failWhen (j1) (_ => true)
      track (AsyncIterator.merge (Seq (i2, j2))) (provided += _)
    }
    expectResult (Set (1)) (c1)
    expectResult (Set (2)) (c2)
    expectResult (Set.empty) (provided)
  }}
