package com.treode.store

import com.treode.cluster.HostId
import com.treode.pickle.Pickler

private case class Residents (nums: Set [Int], mask: Int) {

  def contains (id: Int): Boolean =
    nums contains (id & mask)

  def contains [A] (p: Pickler [A], v: A): Boolean =
    contains (p.murmur32 (v) & mask)
}

private object Residents {

  def apply (host: HostId, cohorts: Array [Cohort]): Residents = {
    val nums = for (c <- cohorts; if c.hosts contains host) yield c.num
    new Residents (nums.toSet, cohorts.size - 1)
  }

  val empty = new Residents (Set.empty, 0)

  val pickler = {
    import StorePicklers._
    wrap (set (uint), uint)
    .build (v => new Residents (v._1, v._2))
    .inspect (v => (v.nums, v.mask))
  }}
