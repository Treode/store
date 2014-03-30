package com.treode.async

import org.scalatest.FlatSpec

import Async.supply
import AsyncImplicits._
import AsyncIteratorTestTools._

class AsyncIteratorSpec extends FlatSpec {

  private def map [A, B] (xs: A*) (f: A => B) (implicit s: StubScheduler): AsyncIterator [B] =
    xs.async.map (f)

  private def filter [A] (xs: A*) (p: A => Boolean) (implicit s: StubScheduler): AsyncIterator [A] =
    xs.async.filter (p)

  "AsyncIterator.foreach" should "handle an empty sequence" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq () (Seq.empty.async)
  }

  it should "handle a sequence of one element" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (1) (adapt (1))
  }

  it should "handle a sequence of three elements" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (1, 2, 3) (adapt (1, 2, 3))
  }

  it should "stop at the first exception " in {
    implicit val scheduler = StubScheduler.random()
    var consumed = Set.empty [Int]
    var provided = Set.empty [Int]
    assertFail [DistinguishedException] {
      val i1 = track (adapt (1, 2, 3, 4)) (consumed += _)
      val i2 = failWhen (i1) (_ == 3)
      track (i2) (provided += _)
    }
    assertResult (Set (1, 2, 3)) (consumed)
    assertResult (Set (1, 2)) (provided)
  }

  it should "work with the for keyword" in {
    var xs = Seq.newBuilder [Int]
    implicit val scheduler = StubScheduler.random()
    val task =
      for (x <- adapt (1, 2, 3))
        supply (xs += x)
    task.pass
    assertResult (Seq (1, 2, 3)) (xs.result)
  }

  "AsyncIterator.map" should "handle an empty sequence" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq () (map [Int, Int] () (_ * 2))
  }

  it should "handle a sequence of one element" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (2) (map (1) (_ * 2))
  }

  it should "handle a sequence of three elements" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (2, 4, 6) (map (1, 2, 3) (_ * 2))
  }

  it should "stop at the first exception" in {
    implicit val scheduler = StubScheduler.random()
    var consumed = Set.empty [Int]
    var provided = Set.empty [Int]
    assertFail [DistinguishedException] {
      val i1 = track (adapt (1, 2, 3, 4)) (consumed += _)
      val i2 = failWhen (i1) (_ == 3)
      val i3 = i2 map (_ * 2)
      track (i3) (provided += _)
    }
    assertResult (Set (1, 2, 3)) (consumed)
    assertResult (Set (2, 4)) (provided)
  }

  it should "work with the for keyword" in {
    implicit val scheduler = StubScheduler.random()
    val iter = for (x <- adapt (1, 2, 3)) yield x * 2
    assertSeq (2, 4, 6) (iter)
  }

  "AsyncIterator.filter" should "handle [] -> []" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq () (filter () (_ => false))
  }

  it should "handle [1] -> []" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq () (filter () (_ => false))
  }

  it should "handle [1] -> [1]" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (1) (filter (1) (_ => true))
  }

  it should "handle [1, 2] -> []" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq () (filter (1, 2) (_ => false))
  }

  it should "handle [1, 2] -> [1, 2]" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (1, 2) (filter (1, 2) (_ => true))
  }

  it should "handle [1, 2] -> [1]" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (1) (filter (1, 2) (i => (i & 1) == 1))
  }

  it should "handle [1, 2] -> [2]" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (2) (filter (1, 2) (i => (i & 1) == 0))
  }

  it should "handle [1, 2, 3] -> []" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq () (filter (1, 2, 3) (_ => false))
  }

  it should "handle [1, 2, 3] -> [1, 2, 3]" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (1, 2, 3) (filter (1, 2, 3) (_ => true))
  }

  it should "handle [1, 2, 3] -> [2]" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (2) (filter (1, 2, 3) (i => (i & 1) == 0))
  }

  it should "handle [1, 2, 3] -> [1, 3]" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (1, 3) (filter (1, 2, 3) (i => (i & 1) == 1))
  }

  it should "handle [1, 2, 3] -> [1, 2]" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (1, 2) (filter (1, 2, 3) (_ < 3))
  }

  it should "handle [1, 2, 3] -> [2, 3]" in {
    implicit val scheduler = StubScheduler.random()
    assertSeq (2, 3) (filter (1, 2, 3) (_ > 1))
  }

  it should "stop at the first exception" in {
    implicit val scheduler = StubScheduler.random()
    var consumed = Set.empty [Int]
    var provided = Set.empty [Int]
    assertFail [DistinguishedException] {
      val i1 = track (adapt (1, 2, 3, 4)) (consumed += _)
      val i2 = failWhen (i1) (_ == 3)
      val i3 = i2.filter (i => (i & 1) == 1)
      track (i3) (provided += _)
    }
    assertResult (Set (1, 2, 3)) (consumed)
    assertResult (Set (1)) (provided)
  }

  it should "work with the for keyword" in {
    implicit val scheduler = StubScheduler.random()
    val iter =
      for (x <- adapt (1, 2, 3, 4); if (x & 1) == 0) yield x
    assertSeq (2, 4) (iter)
  }}
