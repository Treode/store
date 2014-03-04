package com.treode.cluster

import com.treode.async.{Async, Callback, Fiber, Scheduler}
import com.treode.pickle.{InvalidTagException, Pickler, PicklerRegistry}

import Callback.ignore
import PicklerRegistry.{BaseTag, FunctionTag}
import Scuttlebutt.{Handler, Ping, Sync, Universe}

class Scuttlebutt (localId: HostId, peers: PeerRegistry) (implicit scheduler: Scheduler) {

  private val fiber = new Fiber (scheduler)
  private val localHost = peers.get (localId)
  private var universe: Universe = Map.empty .withDefaultValue (Map.empty)
  private var next = 1

  private val mailboxes =
    PicklerRegistry [Handler] { id: Long =>
      PicklerRegistry.const [Peer, Any] (id, ())
    }

  def loopback [M] (desc: RumorDescriptor [M]) (msg: M): Handler =
    mailboxes.loopback (desc.pmsg, desc.id.id, msg)

  def listen [M] (desc: RumorDescriptor [M]) (f: (M, Peer) => Any): Unit =
    PicklerRegistry.tupled (mailboxes, desc.pmsg, desc.id.id) (f)

  def spread [M] (desc: RumorDescriptor [M]) (msg: M): Unit =
    fiber.execute {
      val handler = loopback (desc) (msg)
      universe += (localId -> (universe (localId) + (desc.id -> ((handler, next)))))
      next += 1
    }

  private def _status: Ping =
    for ((host, values) <- universe.toSeq)
      yield (host -> values.map (_._2._2) .max)

  def status: Async [Ping] =
    fiber.supply (_status)

  def ping (hosts: Ping): Async [Sync] =
    fiber.supply {
      val _hosts = hosts.toMap.withDefaultValue (0)
      for {
        (host, state) <- universe.toSeq
        n = _hosts (host)
        deltas = state.values.toSeq.filter (_._2 > n)
        if !deltas.isEmpty
      } yield (host -> deltas)
    }

  def ping (peer: Peer): Unit =
    fiber.execute {
      Scuttlebutt.ping (_status) (peer)
    }

  def sync (updates: Sync): Unit =
    fiber.execute {
      for ((host, deltas) <- updates) {
        val peer = peers.get (host)
        var state = universe (host)
        for ((h1, n1) <- deltas) {
          val k = MailboxId (h1.id)
          val v0 = state.get (k)
          if (v0.isEmpty || v0.get._2 < n1) {
            state += k -> (h1, n1)
            if (next < n1) next = n1 + 1
            scheduler.execute (h1 (peer))
          }}
        universe += host -> state
      }}

  def gab() {
    scheduler.delay (200) {
      peers.rpeer match {
        case Some (peer) => ping (peer)
        case None => ()
      }
      gab()
    }}

  def attach (implicit cluster: Cluster) {

    val _sync = Scuttlebutt.sync (mailboxes.pickler)

    Scuttlebutt.ping.listen { (hosts, from) =>
      val task = for {
        updates <- ping (hosts)
        if !updates.isEmpty
      } yield _sync (updates) (from)
      task run (ignore)
    }

    _sync.listen { (updates, from) =>
      sync (updates)
    }

    gab()
  }}

object Scuttlebutt {

  type Handler = FunctionTag [Peer, Any]
  type Ping = Seq [(HostId, Int)]
  type Sync = Seq [(HostId, Seq [(Handler, Int)])]
  type Universe = Map [HostId, Map [MailboxId, (Handler, Int)]]

  val ping: MessageDescriptor [Ping] = {
    import ClusterPicklers._
    MessageDescriptor (
        0xFF30F8E94A893997L,
        seq (tuple (hostId, uint)))
  }

  def sync (pval: Pickler [Handler]): MessageDescriptor [Sync] = {
    import ClusterPicklers._
    MessageDescriptor (
        0xFF3FBB2A507B2F73L,
        seq (tuple (hostId, seq (tuple (pval, uint)))))
  }}
