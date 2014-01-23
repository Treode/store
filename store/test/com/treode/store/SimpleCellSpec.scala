package com.treode.store

import org.scalatest.FlatSpec

import Fruits.{Apple, Orange}
import SimpleTestTools._

class SimpleCellSpec extends FlatSpec {

  "SimpleCell.compare" should "sort by key" in {
    assert (Apple::None < Orange::None)
  }}
