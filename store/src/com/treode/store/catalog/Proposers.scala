package com.treode.store.catalog

import com.treode.async.Async
import com.treode.cluster.MailboxId
import com.treode.store.Bytes

import Async.async

private class Proposers (kit: CatalogKit) {

  private val proposers = newProposersMap

  def get (key: MailboxId): Proposer = {
    var p0 = proposers.get (key)
    if (p0 != null)
      return p0
    val p1 = new Proposer (key, kit)
    p0 = proposers.putIfAbsent (key, p1)
    if (p0 != null)
      return p0
    p1
  }

  def remove (key: MailboxId, p: Proposer): Unit =
    proposers.remove (key, p)

  def propose (ballot: Long, key: MailboxId, patch: Patch): Async [Update] =
    async { cb =>
      val p = get (key)
      p.open (ballot, patch)
      p.learn (cb)
    }

  def attach() {
    import Proposer.{accept, chosen, promise, refuse}
    import kit.cluster

    refuse.listen { case ((key, ballot), c) =>
      get (key) refuse (ballot)
    }

    promise.listen { case ((key, ballot, proposal), c) =>
      get (key) promise (c, ballot, proposal)
    }

    accept.listen { case ((key, ballot), c) =>
      get (key) accept (c, ballot)
    }

    chosen.listen { case ((key, v), _) =>
      get (key) chosen (v)
    }}}
