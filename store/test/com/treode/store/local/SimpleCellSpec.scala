package com.treode.store.local

import org.scalatest.FlatSpec
import com.treode.store.Fruits

class SimpleCellSpec extends FlatSpec with SimpleTestTools {
  import Fruits.{Apple, Orange}

  "SimpleCell.compare" should "sort by key" in {
    assert (Apple::None < Orange::None)
  }}
