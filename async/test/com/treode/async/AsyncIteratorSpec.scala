package com.treode.async

import org.scalatest.FlatSpec

import AsyncIterator.map
import AsyncIteratorTestTools._

class AsyncIteratorSpec extends FlatSpec {

  "AsyncIterator.scan" should "handle an empty sequence" in {
    expectSeq () (adapt())
  }

  it should "handle a sequence of one element" in {
    expectSeq (1) (adapt (1))
  }

  it should "handle a sequence of three elements" in {
    expectSeq (1, 2, 3) (adapt (1, 2, 3))
  }

  it should "stop at the first exception from hasNext" in {
    var count = 0
    var consumed = Set.empty [Int]
    var provided = Set.empty [Int]
    expectFail [DistinguishedException] {
      val i1 = trackNext (adapt (1, 2, 3, 4)) (consumed += _)
      val i2 = failHasNext (i1) {count+=1; count != 3}
      trackNext (i2) (provided += _)
    }
    expectResult (Set (1, 2)) (consumed)
    expectResult (Set (1, 2)) (provided)
  }

  it should "stop at the first exception from next" in {
    var count = 0
    var consumed = Set.empty [Int]
    var provided = Set.empty [Int]
    expectFail [DistinguishedException] {
      val i1 = trackNext (adapt (1, 2, 3, 4)) (consumed += _)
      val i2 = failNext (i1) {count+=1; count != 3}
      trackNext (i2) (provided += _)
    }
    expectResult (Set (1, 2)) (consumed)
    expectResult (Set (1, 2)) (provided)
  }

  "AsyncIterator.map" should "handle an empty sequence" in {
    expectSeq () (map (adapt [Int] ()) (_ * 2))
  }

  it should "handle a sequence of one element" in {
    expectSeq (2) (map (adapt (1)) (_ * 2))
  }

  it should "handle a sequence of three elements" in {
    expectSeq (2, 4, 6) (map (adapt (1, 2, 3)) (_ * 2))
  }

  it should "stop at the first exception from hasNext" in {
    var count = 0
    var consumed = Set.empty [Int]
    var provided = Set.empty [Int]
    expectFail [DistinguishedException] {
      val i1 = trackNext (adapt (1, 2, 3, 4)) (consumed += _)
      val i2 = failHasNext (i1) {count+=1; count != 3}
      val i3 = map (i2) (_ * 2)
      trackNext (i3) (provided += _)
    }
    expectResult (Set (1, 2)) (consumed)
    expectResult (Set (2, 4)) (provided)
  }

  it should "stop at the first exception from next" in {
    var count = 0
    var consumed = Set.empty [Int]
    var provided = Set.empty [Int]
    expectFail [DistinguishedException] {
      val i1 = trackNext (adapt (1, 2, 3, 4)) (consumed += _)
      val i2 = failNext (i1) {count+=1; count != 3}
      val i3 = map (i2) (_ * 2)
      trackNext (i3) (provided += _)
    }
    expectResult (Set (1, 2)) (consumed)
    expectResult (Set (2, 4)) (provided)
  }}
