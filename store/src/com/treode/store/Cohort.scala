package com.treode.store

import com.treode.async.misc.RichInt
import com.treode.cluster.{ReplyTracker, HostId}

sealed abstract class Cohort {

  def hosts: Set [HostId]
  def track: ReplyTracker
}

object Cohort {

  case class Settled (hosts: Set [HostId]) extends Cohort {

    require (hosts.size.isOdd, "The cohort needs an odd number of hosts.")

    def track: ReplyTracker =
      ReplyTracker.settled (hosts)
  }

  case class Issuing (origin: Set [HostId], target: Set [HostId]) extends Cohort {

    require (origin != target, "This cohort appears settled.")
    require (origin.size.isOdd, "The origin needs an odd number of hosts.")
    require (target.size.isOdd, "The target needs an odd number of hosts.")

    def hosts = origin ++ target

    def track: ReplyTracker =
      ReplyTracker.settled (hosts)
  }

  case class Moving (origin: Set [HostId], target: Set [HostId]) extends Cohort {

    require (origin != target, "This cohort appears settled.")
    require (origin.size.isOdd, "The origin needs an odd number of hosts.")
    require (target.size.isOdd, "The target needs an odd number of hosts.")

    def hosts = origin ++ target

    def track: ReplyTracker =
      ReplyTracker (origin, target)
  }

  def settled (hosts: HostId*): Cohort =
    new Settled (hosts.toSet)

  def issuing (active: HostId*) (target: HostId*): Cohort =
    new Issuing (active.toSet, target.toSet)

  def moving (active: HostId*) (target: HostId*): Cohort =
    new Moving (active.toSet, target.toSet)

  val pickler = {
    import StorePicklers._
    tagged [Cohort] (
        0x1 -> wrap (set (hostId))
               .build (new Settled (_))
               .inspect (_.hosts),
        0x2 -> wrap (set (hostId), set (hostId))
               .build (v => new Issuing (v._1, v._2))
               .inspect (v => (v.origin, v.target)),
        0x3 -> wrap (set (hostId), set (hostId))
               .build (v => new Moving (v._1, v._2))
               .inspect (v => (v.origin, v.target)))
  }}
