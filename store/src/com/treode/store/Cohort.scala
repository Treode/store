package com.treode.store

import com.treode.cluster.{ReplyTracker, HostId}

trait Cohort {

  def hosts: Set [HostId]
  def track: ReplyTracker
}

object Cohort {

  private class Settled (val hosts: Set [HostId]) extends Cohort {

    def track: ReplyTracker =
      ReplyTracker.settled (hosts)
  }

  private class Moving (val origin: Set [HostId], val target: Set [HostId]) extends Cohort {

    def hosts = origin ++ target

    def track: ReplyTracker =
      ReplyTracker (origin, target)
  }

  def apply (active: Set [HostId], target: Set [HostId]): Cohort =
    if (active == target)
      new Settled (active)
    else
      new Moving (active, target)

  def settled (hosts: Set [HostId]): Cohort =
    new Settled (hosts.toSet)

  def settled (hosts: HostId*): Cohort =
    settled (hosts.toSet)

  val pickler = {
    import StorePicklers._
    tagged [Cohort] (
        0x1 -> wrap (set (hostId))
               .build (new Settled (_))
               .inspect (_.hosts),
        0x2 -> wrap (set (hostId), set (hostId))
               .build (v => new Moving (v._1, v._2))
               .inspect (v => (v.origin, v.target)))
  }}
