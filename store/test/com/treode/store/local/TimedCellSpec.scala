package com.treode.store.local

import org.scalatest.FlatSpec
import com.treode.store.{Fruits, TimedCell}

import Fruits.{Apple, Orange}

class TimedCellSpec extends FlatSpec {

  "TimedCell.compare" should "sort by key" in {
    assert (TimedCell (Apple, 1, None) < TimedCell (Orange, 1, None))
    assert ((TimedCell (Apple, 1, None) compare TimedCell (Apple, 1, None)) == 0)
  }

  it should "reverse sort by time" in {
    assert (TimedCell (Apple, 2, None) < TimedCell (Apple, 1, None))
    assert ((TimedCell (Apple, 2, None) compare TimedCell (Apple, 2, None)) == 0)
  }}
