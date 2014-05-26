package com.treode.store.paxos

import com.treode.async.Async
import com.treode.store.{Bytes, TxClock}

import Async.async
import Proposer.{accept, chosen, promise, refuse}

private class Proposers (kit: PaxosKit) {
  import kit.{cluster, releaser}

  val proposers = newProposersMap

  def get (key: Bytes, time: TxClock): Proposer = {
    var p0 = proposers.get ((key, time))
    if (p0 != null) return p0
    val p1 = new Proposer (key, time, kit)
    p0 = proposers.putIfAbsent ((key, time), p1)
    if (p0 != null) return p0
    p1
  }

  def remove (key: Bytes, time: TxClock, p: Proposer): Unit =
    proposers.remove ((key, time), p)

  def propose (ballot: Long, key: Bytes, time: TxClock, value: Bytes): Async [Bytes] =
    releaser.join {
      async { cb =>
        val p = get (key, time)
        p.open (ballot, value)
        p.learn (cb)
      }}

  def attach() {

    refuse.listen { case ((key, time, ballot), c) =>
      get (key, time) refuse (c, ballot)
    }

    promise.listen { case ((key, time, ballot, proposal), c) =>
      get (key, time) promise (c, ballot, proposal)
    }

    accept.listen { case ((key, time, ballot), c) =>
      get (key, time) accept (c, ballot)
    }

    chosen.listen { case ((key, time, v), _) =>
      get (key, time) chosen (v)
    }}}
