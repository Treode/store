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
    for ((key, value) <- recovered)
      okay &&= (accepted.get (key) == Some (value) || attempted.get (key) == Some (value))
    assert (okay, s"Bad recovery.\n$attempted\n$accepted\n$recovered")
  }}
