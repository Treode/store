package com.treode.store

import org.scalatest.FlatSpec

import Fruits.{Apple, Orange}

class CellSpec extends FlatSpec {

  "Cell.compare" should "sort by key" in {
    assert (Cell (Apple, 1, None) < Cell (Orange, 1, None))
    assert ((Cell (Apple, 1, None) compare Cell (Apple, 1, None)) == 0)
  }

  it should "reverse sort by time" in {
    assert (Cell (Apple, 2, None) < Cell (Apple, 1, None))
    assert ((Cell (Apple, 2, None) compare Cell (Apple, 2, None)) == 0)
  }}
