package com.treode.store.local.disk

import scala.collection.immutable.SortedMap

import com.treode.pickle.Picklers

class FreeSet (maxRanges: Int, private var ranges: SortedMap [Int, Int])
extends Iterable [(Int, Int)] {

  def remove(): Int = {
    val range = ranges.head
    ranges -= range._1
    if (range._2 > 1)
      ranges += (range._1 + 1) -> (range._2 - 1)
    range._1
  }

  def add (i: Int): Boolean = {
    val below = ranges .until (i) .lastOption
    val above = ranges .from (i) .headOption
    val addBelow = below.isDefined && below.get._1 + below.get._2 == i
    val addAbove = above.isDefined && above.get._1 - 1 == i
    if (addBelow && addAbove) {
      ranges -= below.get._1
      ranges -= above.get._1
      ranges += below.get._1 -> (below.get._2 + above.get._2 + 1)
      true
    } else if (addBelow) {
      ranges -= below.get._1
      ranges += below.get._1 -> (below.get._2 + 1)
      true
    } else if (addAbove) {
      ranges -= above.get._1
      ranges += i -> (above.get._2 + 1)
      true
    } else if (ranges.size < maxRanges) {
      ranges += i -> 1
      true
    } else {
      false
    }}

  override def isEmpty: Boolean = ranges.isEmpty

  def iterator: Iterator [(Int, Int)] = ranges.iterator
}

object FreeSet {

  def apply (maxRanges: Int): FreeSet =
    new FreeSet (maxRanges, SortedMap [Int, Int] ())

  def pickle (maxRanges: Int) = {
    import Picklers._
    wrap (immutable.sortedMap (int, int))
    .build (new FreeSet (maxRanges, _))
    .inspect (_.ranges)
  }}
