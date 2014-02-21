package com.treode.store.atomic

import com.treode.store.{Bytes, Fruits}
import org.scalatest.FlatSpec

import Fruits.{Boysenberry, Grape}

class TimedKeySpec extends FlatSpec {

  "A TimedKey as bytes" should "preserve the sort of the embedded key" in {
    assert (Boysenberry < Grape)
    assert (Bytes (Bytes.pickler, Boysenberry) > Bytes (Bytes.pickler, Grape))
    assert (TimedKey (Boysenberry, 0) .toBytes < TimedKey (Grape, 0) .toBytes)
  }

  it should "reverse the sort order of time" in {
    assert (TimedKey (Grape, 1) .toBytes < TimedKey (Grape, 0) .toBytes)
  }}
