package com.treode.store.local

import org.scalatest.FlatSpec
import com.treode.store.Fruits

import Fruits.{Apple, Orange}
import LocalSimpleTestTools._

class SimpleCellSpec extends FlatSpec {

  "SimpleCell.compare" should "sort by key" in {
    assert (Apple::None < Orange::None)
  }}
