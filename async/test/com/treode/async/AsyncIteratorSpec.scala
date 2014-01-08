package com.treode.async

import org.scalatest.FlatSpec

class AsyncIteratorSpec extends FlatSpec {

  def translate [A] (xs: Seq [A]): Seq [A] = {
    val cb = new CallbackCaptor [Seq [A]]
    AsyncIterator.scan (AsyncIterator.adapt (xs), cb)
    cb.passed
  }

  "The AsyncIterator" should "handle an empty sequence" in {
    expectResult (Seq.empty) (translate (Seq.empty))
  }

  it should "handle a sequence of one element" in {
    expectResult (Seq (1)) (translate (Seq (1)))
  }

  it should "handle a sequence of three elements" in {
    expectResult (Seq (1, 2, 3)) (translate (Seq (1, 2, 3)))
  }}
