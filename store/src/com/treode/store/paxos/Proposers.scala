package com.treode.store.paxos

import com.treode.async.{Callback, guard}
import com.treode.store.Bytes

private class Proposers (kit: PaxosKit) {

  private val proposers = newProposersMap

  def get (key: Bytes): Proposer = {
    var p0 = proposers.get (key)
    if (p0 != null)
      return p0
    val p1 = new Proposer (key, kit)
    p0 = proposers.putIfAbsent (key, p1)
    if (p0 != null)
      return p0
    p1
  }

  def remove (key: Bytes, p: Proposer): Unit =
    proposers.remove (key, p)

  def propose (ballot: Long, key: Bytes, value: Bytes, cb: Callback [Bytes]): Unit =
    guard (cb) {
      val p = get (key)
      p.open (ballot, value)
      p.learn (cb)
    }

  def attach() {
    import Proposer.{accept, chosen, promise, refuse}
    import kit.cluster

    refuse.register { case ((key, ballot), c) =>
      get (key) refuse (ballot)
    }

    promise.register { case ((key, ballot, proposal), c) =>
      get (key) promise (c, ballot, proposal)
    }

    accept.register { case ((key, ballot), c) =>
      get (key) accept (c, ballot)
    }

    chosen.register { case ((key, v), _) =>
      get (key) chosen (v)
    }}}
