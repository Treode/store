package com.treode.store.tier

import com.treode.store.Fruits
import org.scalatest.FlatSpec

import Fruits.{Apple, Orange}
import TierTestTools._

class TierCellSpec extends FlatSpec {

  "TierCell.compare" should "sort by key" in {
    assert (Apple::None < Orange::None)
  }}
