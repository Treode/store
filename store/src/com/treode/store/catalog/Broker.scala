package com.treode.store.catalog

import com.treode.async.{Async, AsyncConversions, Callback, Fiber, Scheduler}
import com.treode.cluster.{Cluster, MessageDescriptor, Peer}
import com.treode.disk.{Disks, Position, RootDescriptor}
import com.treode.store.{Bytes, Catalogs, CatalogDescriptor, CatalogId, StoreConfig}

import AsyncConversions._
import Broker.Root
import Callback.ignore

private class Broker (
    private var catalogs: Map [CatalogId, Handler]
) (implicit
    scheduler: Scheduler,
    disks: Disks
) {

  private val fiber = new Fiber (scheduler)

  private def _get (id: CatalogId): Handler = {
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

  def get (cat: CatalogId): Async [Handler] =
    fiber.supply {
      _get (cat)
    }

  def diff [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Async [Patch] = {
    val bytes = Bytes (desc.pcat, cat)
    fiber.supply {
      _get (desc.id) diff (version, bytes)
    }}

  def patch (id: CatalogId, update: Update): Async [Unit] =
    fiber.supply {
      _get (id) patch (update)
    }

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
        update = cat.diff (_values (id))
        if !update.isEmpty
      } yield (id -> update)
    }

  def ping (peer: Peer): Unit =
    fiber.execute {
      Broker.ping (_status) (peer)
    }

  def sync (updates: Sync): Unit =
    fiber.execute {
      for ((id, update) <- updates)
        _get (id) .patch (update)
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

  class Root (val catalogs: Map [CatalogId, Position])

  object Root {

    val pickler = {
      import CatalogPicklers._
      wrap (map (catId, pos)) .build (new Root (_)) .inspect (_.catalogs)
    }}

  val root = {
    import CatalogPicklers._
    RootDescriptor (0xB7842D23, Root.pickler)
  }

  val ping: MessageDescriptor [Ping] = {
    import CatalogPicklers._
    MessageDescriptor (
        0xFF8D38A840A7E6BCL,
        seq (tuple (catId, uint)))
  }

  val sync: MessageDescriptor [Sync] = {
    import CatalogPicklers._
    MessageDescriptor (
        0xFF632A972A814B35L,
        seq (tuple (catId, update)))
  }}
