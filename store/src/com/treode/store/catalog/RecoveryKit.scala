package com.treode.store.catalog

import scala.util.Random

import com.treode.async.{Async, AsyncConversions, Fiber, Scheduler}
import com.treode.cluster.Cluster
import com.treode.disk.{Disks, Position}
import com.treode.store.{Atlas, Catalogs, CatalogDescriptor, CatalogId, StoreConfig}

import Async.guard
import AsyncConversions._
import Broker.Root
import Poster.{pager, update}

private class RecoveryKit (implicit
    random: Random,
    scheduler: Scheduler,
    cluster: Cluster,
    recovery: Disks.Recovery,
    config: StoreConfig
) extends Catalogs.Recovery {

  private val fiber = new Fiber (scheduler)
  private var positions = Map.empty [CatalogId, Position]
  private var medics = Map.empty [CatalogId, Medic]
  private var makers = Map.empty [CatalogId, Disks => Poster]

  private def getMedic (id: CatalogId): Medic = {
    medics get (id) match {
      case Some (m) =>
        m
      case None =>
        val m = new Medic
        medics += id -> m
        m
    }}

  private def getMaker (id: CatalogId): Disks => Poster =
    makers get (id) match {
      case Some (m) => m
      case None => (Poster (id) (scheduler, _))
    }

  Broker.root.reload { root =>
    positions ++= root.catalogs
  }

  update.replay { case (id, update) =>
    fiber.execute (getMedic (id) patch (update))
  }

  def listen [C] (desc: CatalogDescriptor [C]) (handler: C => Any): Unit = fiber.execute {
    require (!(makers contains desc.id), f"Catalog ${desc.id.id}%X already registered")
    makers += desc.id -> (Poster (desc, handler) (scheduler, _))
  }

  private def close (id: CatalogId) (implicit disks: Disks): Async [(CatalogId, Handler)] =
    fiber.guard {
      positions.get (id) match {
        case Some (pos) =>
          for {
            (version, bytes, history) <- pager.read (pos)
            result <- fiber.supply {
              val medic = getMedic (id)
              medic.checkpoint (version, bytes, history)
              val maker = getMaker (id) .apply (disks)
              val handler = medic.close (maker)
              (id, handler)
            }
          } yield result
        case None =>
          fiber.supply {
            val medic = getMedic (id)
            val maker = getMaker (id) .apply (disks)
            val handler = medic.close (maker)
            (id, handler)
          }}}

  def launch (implicit launch: Disks.Launch, atlas: Atlas): Async [Catalogs] =
    fiber.guard {
      import launch.disks
      for {
        handlers <- (medics.keySet ++ makers.keySet) .latch.map (close (_))
      } yield {
        val broker = new Broker (handlers)
        broker.attach (cluster)
        new CatalogKit (broker)
      }}}
