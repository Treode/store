package com.treode.store.atomic

import com.treode.store.{Bytes, Fruits}
import org.scalatest.FlatSpec

import Fruits.{Boysenberry, Grape}
import TimedTable.keyToBytes

class TimedTableSpec extends FlatSpec {

  "TimedTable.keyToBytes" should "preserve the sort of the embedded key" in {
    assert (Boysenberry < Grape)
    assert (Bytes (Bytes.pickler, Boysenberry) > Bytes (Bytes.pickler, Grape))
    assert (keyToBytes (Boysenberry, 0) < keyToBytes (Grape, 0))
  }

  it should "reverse the sort order of time" in {
    assert (keyToBytes (Grape, 1) < keyToBytes (Grape, 0))
  }}
