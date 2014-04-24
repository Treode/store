package com.treode.store.catalog

import com.treode.store.CatalogId

private class Acceptors (kit: CatalogKit) {
  import kit.{cluster, disks}

  val acceptors = newAcceptorsMap

  def get (key: CatalogId, version: Int): Acceptor = {
    var a0 = acceptors.get ((key, version))
    if (a0 != null)
      return a0
    val a1 = new Acceptor (key, version, kit)
    a1.state = new a1.Opening
    a0 = acceptors.putIfAbsent ((key, version), a1)
    if (a0 != null)
      return a0
    a1
  }

  def remove (key: CatalogId, version: Int, a: Acceptor): Unit =
    acceptors.remove ((key, version), a)

  def attach() {
    import Acceptor.{choose, propose, query}

    query.listen { case ((key, version, ballot), c) =>
      get (key, version) query (c, ballot)
    }

    propose.listen { case ((key, version, ballot, value), c) =>
      get (key, version) propose (c, ballot, value)
    }

    choose.listen { case ((key, version, chosen), c) =>
      get (key, version) choose (chosen)
    }}}
