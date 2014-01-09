package com.treode.async

import org.scalatest.FlatSpec

class FilteredIteratorSpec extends FlatSpec {

  private def filter [A] (xs: A*) (pred: A => Boolean) = {
    val cb = new CallbackCaptor [AsyncIterator [A]]
    FilteredIterator (AsyncIterator.adapt (xs.iterator), pred, cb)
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
  }}
