package com.treode.store

import com.treode.cluster.HostId
import com.treode.pickle.Pickler

class Residents private [store] (
    private val nums: Set [Int],
    private val mask: Int
) {

  def contains (id: Int): Boolean =
    nums contains (id & mask)

  def contains [A] (p: Pickler [A], v: A): Boolean =
    contains (p.murmur32 (v) & mask)

  def stability (other: Residents): Double = {
    val adjust =
      if (mask == other.mask)
        nums
      else if  (mask < other.mask)
        (0 to other.mask) .filter (i => nums contains (i & mask)) .toSet
      else
        nums .map (_ & other.mask) .toSet
    if (adjust.size == 0)
      1.0D
    else
      (adjust intersect other.nums).size.toDouble / adjust.size.toDouble
  }

  def exodus (other: Residents) (implicit config: StoreConfig): Boolean =
    1 - stability (other) > config.exodusThreshold
}

object Residents {

  val all = new Residents (Set (0), 0)

  val pickler = {
    import StorePicklers._
    wrap (set (uint), uint)
    .build (v => new Residents (v._1, v._2))
    .inspect (v => (v.nums, v.mask))
  }}
