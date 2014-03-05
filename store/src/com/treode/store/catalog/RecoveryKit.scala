package com.treode.store.catalog

import scala.util.Random

import com.treode.async.{Async, AsyncConversions, Fiber, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.{Disks, Position}
import com.treode.store.{Atlas, Catalogs, CatalogDescriptor, CatalogId, StoreConfig}

import Async.supply
import AsyncConversions._
import Broker.root
import Poster.{pager, update}

private class RecoveryKit (implicit
    random: Random,
    scheduler: Scheduler,
    cluster: Cluster,
    recovery: Disks.Recovery,
    config: StoreConfig
) extends Catalogs.Recovery {

  private val fiber = new Fiber (scheduler)
  private var medics = Map.empty [CatalogId, Medic]
  private var makers = Map.empty [CatalogId, Disks => Poster]

  private def getMedic (id: CatalogId): Medic = {
    medics get (id) match {
      case Some (m) =>
        m
      case None =>
        val m = Medic()
        medics += id -> m
        m
    }}

  private def getMaker (id: CatalogId): Disks => Poster =
    makers get (id) match {
      case Some (m) => m
      case None => (Poster (id) (scheduler, _))
    }

  root.reload { root => implicit reload =>
    for {
      _medics <- root.catalogs.latch.map {case (id, pos) => Medic (id, pos)}
    } yield {
      medics = _medics
    }}

  update.replay { case (id, update) =>
    fiber.execute (getMedic (id) patch (update))
  }

  def listen [C] (desc: CatalogDescriptor [C]) (handler: C => Any): Unit = fiber.execute {
    require (!(makers contains desc.id), f"Catalog ${desc.id.id}%X already registered")
    makers += desc.id -> (Poster (desc, handler) (scheduler, _))
  }

  def launch (implicit launch: Disks.Launch, atlas: Atlas): Async [Catalogs] =
    fiber.supply {
      import launch.disks

      val handlers = Map.newBuilder [CatalogId, Handler]
      for (id <- medics.keySet ++ makers.keySet) yield {
        val poster = getMaker (id) .apply (disks)
        val handler = getMedic (id) .close (poster)
        handlers += id -> handler
      }
      val broker = new Broker (handlers.result)
      broker.attach (cluster)
      new CatalogKit (broker)
    }}
