package com.treode.store.atomic

import com.treode.cluster.{ReplyTracker, Peer}
import com.treode.store.Cohort

private class TightTracker [P] (
    _rouse: (Peer, Seq [P]) => Any,
    val acks: Array [ReplyTracker],
    var msgs: Map [Peer, Seq [P]],
    var idxs: Map [Peer, Seq [Int]]
) {

  def += (p: Peer): Unit = {
    acks foreach (_ += p)
    msgs -= p
  }

  def quorum: Boolean =
    acks forall (_.quorum)

  def rouse(): Unit =
    msgs foreach (_rouse.tupled)
}

private object TightTracker {

  def apply [P] (
      ops: Seq [P],
      cohorts: Seq [Cohort],
      kit: AtomicKit
  ) (
      rouse: (Peer, Seq [P]) => Any
  ): TightTracker [P] = {
    import kit.cluster.peer

    val cidxs = cohorts.zipWithIndex groupBy (_._1)

    var pidxs = Map.empty [Peer, Seq [Int]] .withDefaultValue (Seq.empty)
    for ((cohort, xs) <- cidxs.toSeq; host <- cohort.hosts) {
      val p = peer (host)
      pidxs += p -> (pidxs (p) ++ (xs map (_._2)))
    }

    val pops = pidxs mapValues (_.map (ops (_)))

    val acks = cidxs.keys.map (_.track) .toArray

    new TightTracker (rouse, acks, pops, pidxs)
  }}
