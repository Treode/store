package com.treode.async

import org.scalatest.FlatSpec

import AsyncIteratorTestTools._

class FilteredIteratorSpec extends FlatSpec {

  private def filter [A] (xs: A*) (pred: A => Boolean): AsyncIterator [A] = {
    val cb = new CallbackCaptor [AsyncIterator [A]]
    AsyncIterator.filter (AsyncIterator.adapt (xs), cb) (pred)
    cb.passed
  }

  private def filter [A] (xs: AsyncIterator [A]) (pred: A => Boolean): AsyncIterator [A] = {
    val cb = new CallbackCaptor [AsyncIterator [A]]
    AsyncIterator.filter (xs, cb) (pred)
    cb.passed
  }

  private def expectSeq [A] (xs: A*) (actual: AsyncIterator [A]) {
    val cb = new CallbackCaptor [Seq [A]]
    AsyncIterator.scan (actual, cb)
    expectResult (xs) (cb.passed)
  }

  "The FilteredIterator" should "handle [] -> []" in {
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

  it should "stop at the first exception from hasNext" in {
    var count = 0
    var consumed = Set.empty [Int]
    var provided = Set.empty [Int]
    expectFail [DistinguishedException] {
      val i1 = trackNext (adapt (1, 2, 3, 4)) (consumed += _)
      val i2 = failNext (i1) {count+=1; count != 3}
      val i3 = filter (i2) (_ => true)
      trackNext (i3) (provided += _)
    }
    expectResult (Set (1, 2)) (consumed)
    expectResult (Set (1)) (provided)
  }

  it should "stop at the first exception from next" in {
    var count = 0
    var consumed = Set.empty [Int]
    var provided = Set.empty [Int]
    expectFail [DistinguishedException] {
      val i1 = trackNext (adapt (1, 2, 3, 4)) (consumed += _)
      val i2 = failNext (i1) {count+=1; count != 3}
      val i3 = filter (i2) (_ => true)
      trackNext (i3) (provided += _)
    }
    expectResult (Set (1, 2)) (consumed)
    expectResult (Set (1)) (provided)
  }}
