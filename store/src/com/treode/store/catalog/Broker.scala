package com.treode.store.catalog

import com.treode.async.{Async, AsyncImplicits, Callback, Fiber, Scheduler}
import com.treode.buffer.ArrayBuffer
import com.treode.cluster.{Cluster, MessageDescriptor, Peer}
import com.treode.disk.{Disks, ObjectId, PageDescriptor, PageHandler, Position}
import com.treode.store.{Bytes, CatalogDescriptor, CatalogId}
import com.treode.pickle.PicklerRegistry

import Async.guard
import AsyncImplicits._
import Callback.ignore
import Handler.pager

private class Broker (
    private var catalogs: Map [CatalogId, Handler]
) (implicit
    scheduler: Scheduler,
    disks: Disks
) extends PageHandler [Int] {

  private val fiber = new Fiber (scheduler)

  private val ports = PicklerRegistry [Any] {id: Long => ()}

  private def _get (id: CatalogId): Handler = {
    catalogs get (id) match {
      case Some (cat) =>
        cat
      case None =>
        val cat = Handler (id)
        catalogs += id -> cat
        cat
    }}

  private def deliver (id: CatalogId, cat: Handler): Unit =
    scheduler.execute {
      ports.unpickle (id.id, ArrayBuffer (cat.bytes.bytes))
    }

  def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any): Unit =
    fiber.execute {
      ports.register (desc.pcat, desc.id.id) (f)
      catalogs get (desc.id) match {
        case Some (cat) => deliver (desc.id, cat)
        case None => ()
      }}

  def get (cat: CatalogId): Async [Handler] =
    fiber.supply {
      _get (cat)
    }

  def diff [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Async [Patch] =
    guard {
      val bytes = Bytes (desc.pcat, cat)
      fiber.supply {
        _get (desc.id) diff (version, bytes)
      }}

  def patch (id: CatalogId, update: Update): Async [Unit] =
    fiber.supply {
      val cat = _get (id)
      if (cat.patch (update))
        deliver (id, cat)
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
      for ((id, update) <- updates) {
        val cat = _get (id)
        if (cat.patch (update))
          deliver (id, cat)
      }}

  def gab () (implicit cluster: Cluster) {
    scheduler.delay (200) {
      cluster.rpeer match {
        case Some (peer) => ping (peer)
        case None => ()
      }
      gab()
    }}

  def probe (obj: ObjectId, groups: Set [Int]): Async [Set [Int]] =
    fiber.supply {
      _get (obj.id) .probe (groups)
    }

  def compact (obj: ObjectId, groups: Set [Int]): Async [Unit] =
    fiber.guard {
      _get (obj.id) .compact (groups)
    }

  def checkpoint(): Async [Unit] =
    fiber.guard {
      catalogs.values.latch.unit foreach (_.checkpoint())
    }

  def attach () (implicit launch: Disks.Launch, cluster: Cluster) {

    pager.handle (this)

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
