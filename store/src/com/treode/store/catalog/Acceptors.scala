package com.treode.store.catalog

import com.treode.async.{Async, AsyncConversions, Latch}
import com.treode.disk.{PageDescriptor, Position, RootDescriptor}
import com.treode.store.{Bytes, CatalogId}
import com.treode.store.tier.{TierDescriptor, TierTable}

import Async.guard
import AsyncConversions._

private class Acceptors (kit: CatalogKit) {
  import kit.{cluster, disks}

  val acceptors = newAcceptorsMap

  def get (key: CatalogId): Acceptor = {
    var a0 = acceptors.get (key)
    if (a0 != null)
      return a0
    val a1 = new Acceptor (key, kit)
    a1.state = new a1.Opening
    a0 = acceptors.putIfAbsent (key, a1)
    if (a0 != null)
      return a0
    a1
  }

  def remove (key: CatalogId, a: Acceptor): Unit =
    acceptors.remove (key, a)

  def attach() {
    import Acceptor.{choose, propose, query}

    query.listen { case ((key, ballot), c) =>
      get (key) query (c, ballot)
    }

    propose.listen { case ((key, ballot, value), c) =>
      get (key) propose (c, ballot, value)
    }

    choose.listen { case ((key, chosen), c) =>
      get (key) choose (chosen)
    }}}
