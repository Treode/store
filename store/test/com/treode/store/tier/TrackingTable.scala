package com.treode.store.tier

class TrackingTable {

  private var attempted = Map.empty [Int, Option [Int]]
  private var accepted = Map.empty [Int, Option [Int]]

  def putting (key: Int, value: Int): Unit =
    attempted += key -> Some (value)

  def put (key: Int, value: Int): Unit =
    accepted += key -> Some (value)

  def deleting (key: Int): Unit =
    attempted += key -> None

  def deleted (key: Int): Unit =
    accepted += key -> None

  def check (recovered: Map [Int, Int]) {
    for (k <- accepted.keySet)
      assert (
          recovered.contains (k) || attempted (k) == None,
          s"Expected $k to be recovered")
    for ((k, v) <- recovered)
      assert (
          attempted (k) == Some (v) || accepted (k) == Some (v),
          s"Expected $k to be ${attempted (k)} or ${accepted (k)}")
  }}
