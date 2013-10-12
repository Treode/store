package com.treode.store.simple

import org.scalatest.FlatSpec
import com.treode.store.Fruits

class SimpleCellSpec extends FlatSpec with TestTools {
  import Fruits.{Apple, Orange}

  "Cell.compare" should "sort by key" in {
    assert (Apple::None < Orange::None)
  }}
