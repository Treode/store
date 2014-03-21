package com.treode.store.tier

class TrackingTable {

  private var attempted = Map.empty [Int, Int]
  private var accepted = Map.empty [Int, Int]

  def putting (key: Int, value: Int): Unit =
    attempted += (key -> value)

  def put (key: Int, value: Int): Unit =
    accepted += (key -> value)

  def deleting (key: Int): Unit =
    attempted -= key

  def deleted (key: Int): Unit =
    accepted -= key

  def check (recovered: Map [Int, Int]) {
    var okay = true
    for ((key, value) <- recovered) {
      val _accepted = accepted.get (key)
      val _attempted = attempted.get (key)
      val expected = Seq (_accepted, _attempted) .flatten.mkString (" or ")
      assert (
          _accepted == Some (value) || _attempted == Some (value),
          s"Found $key -> $value, expected $expected")
    }
    for ((key, value) <- accepted) {
      val _recovered = recovered.get (key)
      val _attempted = attempted.get (key)
      assert (
          _recovered == Some (value) || _recovered == _attempted,
          s"Found ${_recovered}, expected ${Some (value)} or ${_attempted}")
    }}}
