package com.treode.store.atomic

import com.treode.cluster.Peer
import com.treode.store.Cohort

private class BroadTracker (_rouse: Set [Peer] => Any, var hosts: Set [Peer]) {

  def += (p: Peer): Unit =
    hosts -= p

  def unity: Boolean =
    hosts.isEmpty

  def rouse(): Unit =
    _rouse (hosts)
}

private object BroadTracker {

  def apply (cohorts: Seq [Cohort], kit: AtomicKit) (rouse: Set [Peer] => Any): BroadTracker = {
    import kit.cluster.peer
    val hosts = cohorts.map (_.hosts) .fold (Set.empty) (_ | _) .map (peer (_))
    new BroadTracker (rouse, hosts)
  }}
