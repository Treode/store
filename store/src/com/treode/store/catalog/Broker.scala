package com.treode.store.catalog

import com.treode.async.{Async, Callback, Fiber, Scheduler}
import com.treode.cluster.{Cluster, MailboxId, MessageDescriptor, Peer}
import com.treode.store.{Bytes, StoreConfig, StorePicklers}

import Callback.ignore

private class Broker (implicit scheduler: Scheduler) {

  private val fiber = new Fiber (scheduler)
  private var catalogs = Map.empty [MailboxId, CatalogHandler]

  private def get (id: MailboxId): CatalogHandler = {
    catalogs get (id) match {
      case Some (cat) =>
        cat
      case None =>
        val cat = CatalogHandler.unknown (id)
        catalogs += id -> cat
        cat
    }}

  def listen [C] (desc: CatalogDescriptor [C]) (handler: C => Any): Unit = fiber.execute {
    require (!(catalogs contains desc.id), f"Catalog ${desc.id.id}%X already registered")
    catalogs += desc.id -> CatalogHandler (desc.id, desc.pcat, handler)
  }

  def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C) {
    val _cat = Bytes (desc.pcat, cat)
    fiber.execute {
      get (desc.id) issue (version, _cat)
    }}

  private def _status: Ping =
    for ((id, cat) <- catalogs.toSeq)
      yield (id, cat.version)

  def status: Async [Ping] =
    fiber.supply (_status)

  def ping (values: Ping): Async [Sync] =
    fiber.supply {
      val _values = values.toMap.withDefaultValue (0)
      for {
        (id, cat) <- catalogs.toSeq
        u = cat.diff (_values (id))
        if !isEmpty (u)
      } yield (id -> u)
    }

  def ping (peer: Peer): Unit =
    fiber.execute {
      Broker.ping (_status) (peer)
    }


  def sync (updates: Sync): Unit =
    fiber.execute {
      for ((id, update) <- updates)
        get (id) .patch (update)
    }

  def gab () (implicit cluster: Cluster) {
    scheduler.delay (200) {
      cluster.rpeer match {
        case Some (peer) => ping (peer)
        case None => ()
      }
      gab()
    }}

  def attach (implicit cluster: Cluster) {

    Broker.ping.listen { (values, from) =>
      val task = for {
        updates <- ping (values)
        if !updates.isEmpty
      } yield Broker.sync (updates) (from)
      task run (ignore)
    }

    Broker.sync.listen { (updates, from) =>
      sync (updates)
    }

    gab()
  }}

private object Broker {

  val ping: MessageDescriptor [Ping] = {
    import StorePicklers._
    MessageDescriptor (
        0xFF8D38A840A7E6BCL,
        seq (tuple (mbxId, uint)))
  }

  val sync: MessageDescriptor [Sync] = {
    import StorePicklers._
    val patches = seq (bytes)
    val value = tuple (uint, bytes, patches)
    val update = either (value, tuple (uint, patches))
    MessageDescriptor (
        0xFF632A972A814B35L,
        seq (tuple (mbxId, update)))
  }}
