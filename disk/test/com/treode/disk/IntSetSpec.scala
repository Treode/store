package com.treode.disk

import org.scalatest.FlatSpec

class IntSetSpec extends FlatSpec {

  def assertSet (xs: Int*) (s: IntSet): Unit =
    assertResult (xs.toSet) (s.toSet)

  "An IntSet" should "fill with 0 to n" in {
    val s = IntSet.fill (4)
    assertSet (0, 1, 2, 3) (s)
  }

  it should "remove a member" in {
    val s0 = IntSet.fill (4)
    val s1 = s0.remove (0)
    val s2 = s0.remove (1)
    val s3 = s1.remove (2)
    assertSet (0, 1, 2, 3) (s0)
    assertSet (1, 2, 3) (s1)
    assertSet (0, 2, 3) (s2)
    assertSet (1, 3) (s3)
  }

  it should "add a member" in {
    val s0 = IntSet.fill (0)
    val s1 = s0.add (0)
    val s2 = s0.add (1)
    val s3 = s1.add (2)
    assertSet () (s0)
    assertSet (0) (s1)
    assertSet (1) (s2)
    assertSet (0, 2) (s3)
  }

  it should "complement the set" in {
    val s0 = IntSet.fill (4)
    val s1 = s0.remove (0)
    val s2 = s1.complement
    assertSet (0) (s2)
    val s3 = s1.remove (2)
    val s4 = s3.complement
    assertSet (0, 2) (s4)
  }

  it should "pickle" in {
    import IntSet.pickler.{fromByteArray, toByteArray}

    def checkPickle (s: IntSet) {
      val s2 = fromByteArray (toByteArray (s))
      assertResult (s) (s2)
    }

    checkPickle (IntSet())
    checkPickle (IntSet.fill (4))
    checkPickle (IntSet.fill (1<<30))
    checkPickle (IntSet.fill (4) .remove (0) .remove (2))
  }}
