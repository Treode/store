package com.treode.async

import org.scalatest.FlatSpec

import AsyncIteratorTestTools._

class AsyncIteratorSpec extends FlatSpec {

  private def map [A, B] (xs: A*) (f: A => B): AsyncIterator [B] =
    adapt (xs: _*) .map (f)

  private def filter [A] (xs: A*) (pred: A => Boolean): AsyncIterator [A] =
    adapt (xs: _*) .filter (pred)

  "AsyncIterator.foreach" should "handle an empty sequence" in {
    expectSeq () (adapt())
  }

  it should "handle a sequence of one element" in {
    expectSeq (1) (adapt (1))
  }

  it should "handle a sequence of three elements" in {
    expectSeq (1, 2, 3) (adapt (1, 2, 3))
  }

  it should "stop at the first exception " in {
    var consumed = Set.empty [Int]
    var provided = Set.empty [Int]
    expectFail [DistinguishedException] {
      val i1 = track (adapt (1, 2, 3, 4)) (consumed += _)
      val i2 = failWhen (i1) (_ == 3)
      track (i2) (provided += _)
    }
    expectResult (Set (1, 2, 3)) (consumed)
    expectResult (Set (1, 2)) (provided)
  }

  "AsyncIterator.map" should "handle an empty sequence" in {
    expectSeq () (map [Int, Int] () (_ * 2))
  }

  it should "handle a sequence of one element" in {
    expectSeq (2) (map (1) (_ * 2))
  }

  it should "handle a sequence of three elements" in {
    expectSeq (2, 4, 6) (map (1, 2, 3) (_ * 2))
  }

  it should "stop at the first exception" in {
    var consumed = Set.empty [Int]
    var provided = Set.empty [Int]
    expectFail [DistinguishedException] {
      val i1 = track (adapt (1, 2, 3, 4)) (consumed += _)
      val i2 = failWhen (i1) (_ == 3)
      val i3 = i2 map (_ * 2)
      track (i3) (provided += _)
    }
    expectResult (Set (1, 2, 3)) (consumed)
    expectResult (Set (2, 4)) (provided)
  }

  "AsyncIterator.filter" should "handle [] -> []" in {
    expectSeq () (filter () (_ => false))
  }

  it should "handle [1] -> []" in {
    expectSeq () (filter () (_ => false))
  }

  it should "handle [1] -> [1]" in {
    expectSeq (1) (filter (1) (_ => true))
  }

  it should "handle [1, 2] -> []" in {
    expectSeq () (filter (1, 2) (_ => false))
  }

  it should "handle [1, 2] -> [1, 2]" in {
    expectSeq (1, 2) (filter (1, 2) (_ => true))
  }

  it should "handle [1, 2] -> [1]" in {
    expectSeq (1) (filter (1, 2) (i => (i & 1) == 1))
  }

  it should "handle [1, 2] -> [2]" in {
    expectSeq (2) (filter (1, 2) (i => (i & 1) == 0))
  }

  it should "handle [1, 2, 3] -> []" in {
    expectSeq () (filter (1, 2, 3) (_ => false))
  }

  it should "handle [1, 2, 3] -> [1, 2, 3]" in {
    expectSeq (1, 2, 3) (filter (1, 2, 3) (_ => true))
  }

  it should "handle [1, 2, 3] -> [2]" in {
    expectSeq (2) (filter (1, 2, 3) (i => (i & 1) == 0))
  }

  it should "handle [1, 2, 3] -> [1, 3]" in {
    expectSeq (1, 3) (filter (1, 2, 3) (i => (i & 1) == 1))
  }

  it should "handle [1, 2, 3] -> [1, 2]" in {
    expectSeq (1, 2) (filter (1, 2, 3) (_ < 3))
  }

  it should "handle [1, 2, 3] -> [2, 3]" in {
    expectSeq (2, 3) (filter (1, 2, 3) (_ > 1))
  }

  it should "stop at the first exception" in {
    var consumed = Set.empty [Int]
    var provided = Set.empty [Int]
    expectFail [DistinguishedException] {
      val i1 = track (adapt (1, 2, 3, 4)) (consumed += _)
      val i2 = failWhen (i1) (_ == 3)
      val i3 = i2.filter (i => (i & 1) == 1)
      track (i3) (provided += _)
    }
    expectResult (Set (1, 2, 3)) (consumed)
    expectResult (Set (1)) (provided)
  }}
