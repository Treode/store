package com.treode.store.local

import org.scalatest.FlatSpec
import com.treode.store.{Fruits, SimpleTestTools}

import Fruits.{Apple, Orange}
import SimpleTestTools._

class SimpleCellSpec extends FlatSpec {

  "SimpleCell.compare" should "sort by key" in {
    assert (Apple::None < Orange::None)
  }}
