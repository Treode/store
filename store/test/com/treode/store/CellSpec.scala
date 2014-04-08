package com.treode.store

import org.scalatest.FlatSpec

import Fruits.{Apple, Orange}
import TimedTestTools._

class CellSpec extends FlatSpec {

  "Cell.compare" should "sort by key" in {
    assert (Apple##1 < Orange##1)
    assert ((Apple##1 compare Apple##1) == 0)
  }

  it should "reverse sort by time" in {
    assert (Apple##2 < Apple##1)
  }}
