package com.treode.store

import com.treode.async.misc.RichInt
import com.treode.cluster.{ReplyTracker, HostId}

import Cohort._

sealed abstract class Cohort {

  def hosts: Set [HostId]
  def origin: Set [HostId]
  def target: Set [HostId]
  def track: ReplyTracker
  def quorum (acks: Set [HostId]): Boolean

  def contains (host: HostId): Boolean =
    hosts contains (host)

  def settled: Boolean = this.isInstanceOf [Cohort.Settled]
  def issuing: Boolean = this.isInstanceOf [Issuing]
  def moving: Boolean = this.isInstanceOf [Moving]
}

object Cohort {

  case object Empty extends Cohort {

    def hosts = Set.empty
    def origin = Set.empty
    def target = Set.empty

    def track: ReplyTracker =
      ReplyTracker.empty

    def quorum (acks: Set [HostId]): Boolean =
      false
  }

  case class Settled (hosts: Set [HostId]) extends Cohort {

    val nquorum = (hosts.size >> 1) + 1

    require (hosts.size.isOdd, "The cohort needs an odd number of hosts.")

    def origin = hosts
    def target = hosts

    def track: ReplyTracker =
      ReplyTracker.settled (hosts)

    def quorum (acks: Set [HostId]): Boolean =
      hosts.count (acks contains _) >= nquorum
  }

  case class Issuing (origin: Set [HostId], target: Set [HostId]) extends Cohort {

    require (origin != target, "This cohort appears settled.")
    require (origin.size.isOdd, "The origin needs an odd number of hosts.")
    require (target.size.isOdd, "The target needs an odd number of hosts.")

    val oquorum = (origin.size >> 1) + 1
    val tquorum = (target.size >> 1) + 1

    def hosts = origin ++ target

    def track: ReplyTracker =
      ReplyTracker (origin, target)

    def quorum (acks: Set [HostId]): Boolean =
      origin.count (acks contains _) >= oquorum && target.count (acks contains _) >= tquorum
  }

  case class Moving (origin: Set [HostId], target: Set [HostId]) extends Cohort {

    require (origin != target, "This cohort appears settled.")
    require (origin.size.isOdd, "The origin needs an odd number of hosts.")
    require (target.size.isOdd, "The target needs an odd number of hosts.")

    val oquorum = (origin.size >> 1) + 1
    val tquorum = (target.size >> 1) + 1

    def hosts = origin ++ target

    def track: ReplyTracker =
      ReplyTracker (origin, target)

    def quorum (acks: Set [HostId]): Boolean =
      origin.count (acks contains _) >= oquorum && target.count (acks contains _) >= tquorum
  }

  def settled (hosts: HostId*): Cohort =
    new Settled (hosts.toSet)

  def issuing (active: HostId*) (target: HostId*): Cohort =
    new Issuing (active.toSet, target.toSet)

  def moving (active: HostId*) (target: HostId*): Cohort =
    new Moving (active.toSet, target.toSet)

  val empty = Empty

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
