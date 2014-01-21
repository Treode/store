package com.treode.store.cluster.paxos

import java.util.concurrent.ConcurrentHashMap

import com.treode.async.{Callback, guard}
import com.treode.cluster.Host
import com.treode.store.{Bytes, PaxosStore, SimpleAccessor, SimpleStore}
import com.treode.disk.Disks

private class PaxosKit (implicit val host: Host, val disks: Disks) extends PaxosStore {
  import host.random

  object Acceptors {
    import Acceptor._

    private val acceptors = new ConcurrentHashMap [Bytes, Acceptor] ()

    def get (key: Bytes): Acceptor = {
      var a0 = acceptors.get (key)
      if (a0 != null)
        return a0
      val a1 = new Acceptor (key, PaxosKit.this)
      a0 = acceptors.putIfAbsent (key, a1)
      if (a0 != null)
        return a0
      a1
    }

    def remove (key: Bytes, a: Acceptor) =
      acceptors.remove (key, a)

    def locate (key: Bytes) =
      host.locate (key.hashCode)

    query.register { case ((key, ballot, default), c) =>
      get (key) query (c, ballot, default)
    }

    Acceptor.propose.register { case ((key, ballot, value), c) =>
      get (key) propose (c, ballot, value)
    }

    choose.register { case ((key, value), c) =>
      get (key) choose (value)
    }}

  object Proposers {
    import Proposer._

    private val proposers = new ConcurrentHashMap [Bytes, Proposer]

    def get (key: Bytes): Proposer = {
      var p0 = proposers.get (key)
      if (p0 != null)
        return p0
      val p1 = new Proposer (key, PaxosKit.this)
      p0 = proposers.putIfAbsent (key, p1)
      if (p0 != null)
        return p0
      p1
    }

    def remove (key: Bytes, p: Proposer) =
      proposers.remove (key, p)

    def propose (ballot: Long, key: Bytes, value: Bytes, cb: Callback [Bytes]): Unit =
      guard (cb) {
        val p = get (key)
        p.open (ballot, value)
        p.learn (cb)
      }

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
    }}

  Acceptors
  Proposers

  def lead (key: Bytes, value: Bytes, cb: Callback [Bytes]): Unit =
    guard (cb) {
      Proposers.propose (0, key, value, cb)
    }

  def propose (key: Bytes, value: Bytes, cb: Callback [Bytes]): Unit =
    guard (cb) {
      Proposers.propose (random.nextInt (17) + 1, key, value, cb)
    }

  def close() = ()
}

private [store] object PaxosKit {

  def apply (host: Host, disks: Disks): PaxosStore =
    new PaxosKit () (host, disks)
}
