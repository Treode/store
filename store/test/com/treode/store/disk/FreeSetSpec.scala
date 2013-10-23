package com.treode.store.disk

import org.scalatest.FlatSpec

class FreeSetSpec extends FlatSpec {

  def expectSeq [T] (xs: T*) (iter: Iterable [T]): Unit =
    expectResult (Seq (xs: _*)) (iter.toSeq)

  "Thee FreeSet" should "add to an empty set" in {
    val free = FreeSet (16)
    free.add (11)
    expectSeq ((11, 1)) (free)
  }

  it should "add way before a singleton" in {
    val free = FreeSet (16)
    free.add (11)
    free.add (1)
    expectSeq ((1, 1), (11, 1)) (free)
  }

  it should "add way after a singleton" in {
    val free = FreeSet (16)
    free.add (11)
    free.add (21)
    expectSeq ((11, 1), (21, 1)) (free)
  }

  it should "add immediately before a singleton" in {
    val free = FreeSet (16)
    free.add (11)
    free.add (10)
    expectSeq ((10, 2)) (free)
  }

  it should "add immediately after a singleton" in {
    val free = FreeSet (16)
    free.add (11)
    free.add (12)
    expectSeq ((11, 2)) (free)
  }

  it should "add between two ranges" in {
    val free = FreeSet (16)
    free.add (1)
    free.add (21)
    free.add (11)
    expectSeq ((1, 1), (11, 1), (21, 1)) (free)
  }

  it should "add immediately between two ranges" in {
    val free = FreeSet (16)
    free.add (1)
    free.add (3)
    free.add (2)
    expectSeq ((1, 3)) (free)
  }

  it should "reject a non-contiguous element when full" in {
    val free = FreeSet (2)
    free.add (1)
    free.add (21)
    expectResult (false) (free.add (11))
    expectSeq ((1, 1), (21, 1)) (free)
  }

  it should "accept a contiguous element above when full" in {
    val free = FreeSet (2)
    free.add (1)
    free.add (21)
    expectResult (true) (free.add (2))
    expectSeq ((1, 2), (21, 1)) (free)
  }

  it should "accept a contiguous element below when full" in {
    val free = FreeSet (2)
    free.add (1)
    free.add (21)
    expectResult (true) (free.add (20))
    expectSeq ((1, 1), (20, 2)) (free)
  }

  it should "accept a contiguous element that merges when full" in {
    val free = FreeSet (2)
    free.add (1)
    free.add (3)
    expectResult (true) (free.add (2))
    expectSeq ((1, 3)) (free)
  }

  it should "remove from a singleton and leave the set empty" in {
    val free = FreeSet (16)
    free.add (11)
    expectResult (11) (free.remove())
    expectSeq () (free)
  }

  it should "remove one from a range" in {
    val free = FreeSet (16)
    free.add (11)
    free.add (12)
    expectResult (11) (free.remove())
    expectSeq ((12, 1)) (free)
  }}
