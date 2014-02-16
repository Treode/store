package com.treode.store.tier

import com.treode.store.Fruits
import org.scalatest.FlatSpec

import Fruits.{Apple, Orange}
import TierTestTools._

class CellSpec extends FlatSpec {

  "Cell.compare" should "sort by key" in {
    assert (Apple::None < Orange::None)
  }}
