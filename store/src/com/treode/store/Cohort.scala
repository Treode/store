package com.treode.store

import com.treode.cluster.{ReplyTracker, HostId}

sealed abstract class Cohort {

  def num: Int
  def hosts: Set [HostId]
  def track: ReplyTracker
}

object Cohort {

  case class Settled (num: Int, hosts: Set [HostId]) extends Cohort {

    def track: ReplyTracker =
      ReplyTracker.settled (hosts)
  }

  case class Issuing (num: Int, origin: Set [HostId], target: Set [HostId]) extends Cohort {

    def hosts = origin ++ target

    def track: ReplyTracker =
      ReplyTracker.settled (hosts)
  }

  case class Moving (num: Int, origin: Set [HostId], target: Set [HostId]) extends Cohort {

    def hosts = origin ++ target

    def track: ReplyTracker =
      ReplyTracker (origin, target)
  }

  def apply (num: Int, active: Set [HostId], target: Set [HostId]): Cohort =
    if (active == target)
      new Settled (num, active)
    else
      new Moving (num, active, target)

  def settled (num: Int, hosts: Set [HostId]): Cohort =
    new Settled (num, hosts)

  def settled (num: Int, hosts: HostId*): Cohort =
    settled (num, hosts.toSet)

  val pickler = {
    import StorePicklers._
    tagged [Cohort] (
        0x1 -> wrap (uint, set (hostId))
               .build (v => new Settled (v._1, v._2))
               .inspect (v => (v.num, v.hosts)),
        0x2 -> wrap (uint, set (hostId), set (hostId))
               .build (v => new Issuing (v._1, v._2, v._3))
               .inspect (v => (v.num, v.origin, v.target)),
        0x3 -> wrap (uint, set (hostId), set (hostId))
               .build (v => new Moving (v._1, v._2, v._3))
               .inspect (v => (v.num, v.origin, v.target)))
  }}
