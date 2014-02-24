package com.treode.store.atomic

import java.util.concurrent.ConcurrentHashMap

import com.treode.store.{Bytes, TxId}

private class WriteDeputies (kit: AtomicKit) {
  import WriteDeputy._
  import kit.cluster

  private val deputies = new ConcurrentHashMap [Bytes, WriteDeputy] ()

  def get (xid: TxId): WriteDeputy = {
    var d0 = deputies.get (xid.id)
    if (d0 != null)
      return d0
    val d1 = new WriteDeputy (xid, kit)
    d0 = deputies.putIfAbsent (xid.id, d1)
    if (d0 != null)
      return d0
    d1
  }

  def attach() {

    prepare.listen { case ((xid, ct, ops), mdtr) =>
      get (xid) .prepare (mdtr, ct, ops)
    }

    commit.listen { case ((xid, wt), mdtr) =>
      get (xid) .commit (mdtr, wt)
    }

    abort.listen { case (xid, mdtr) =>
      get (xid) .abort (mdtr)
    }}}
