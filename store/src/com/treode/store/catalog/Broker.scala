package com.treode.store.catalog

import com.treode.async.{Async, AsyncConversions, Callback, Fiber, Scheduler}
import com.treode.cluster.{Cluster, MailboxId, MessageDescriptor, Peer}
import com.treode.disk.{Disks, Position, RootDescriptor}
import com.treode.store.{Bytes, StoreConfig, StorePicklers}

import AsyncConversions._
import Broker.Root
import Callback.ignore

private class Broker (
    private var catalogs: Map [MailboxId, Handler]
) (implicit
    scheduler: Scheduler,
    disks: Disks
) extends Catalogs {

  private val fiber = new Fiber (scheduler)

  private def get (id: MailboxId): Handler = {
    catalogs get (id) match {
      case Some (cat) =>
        cat
      case None =>
        val cat = Handler (Poster (id))
        catalogs += id -> cat
        cat
    }}

  def listen [C] (desc: CatalogDescriptor [C]) (handler: C => Any): Unit = fiber.execute {
    require (!(catalogs contains desc.id), f"Catalog ${desc.id.id}%X already registered")
    catalogs += desc.id -> Handler (Poster (desc, handler))
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

  def checkpoint(): Async [Root] =
    fiber.guard {
      for {
        _catalogs <- catalogs.values.latch.map (_.checkpoint())
      } yield {
        new Root (_catalogs)
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

object Broker {

  class Root (val catalogs: Map [MailboxId, Position])

  object Root {

    val pickler = {
      import StorePicklers._
      wrap (map (mbxId, pos)) .build (new Root (_)) .inspect (_.catalogs)
    }}

  val root = {
    import StorePicklers._
    RootDescriptor (0xB7842D23, Root.pickler)
  }

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
