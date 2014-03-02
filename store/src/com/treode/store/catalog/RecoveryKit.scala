package com.treode.store.catalog

import com.treode.async.{Async, AsyncConversions, Fiber, Scheduler}
import com.treode.cluster.{Cluster, MailboxId}
import com.treode.disk.{Disks, Position}

import Async.supply
import AsyncConversions._
import Broker.root
import Poster.{pager, update}

class RecoveryKit (implicit
    scheduler: Scheduler,
    cluster: Cluster,
    recovery: Disks.Recovery
) extends Catalogs.Recovery {

  private val fiber = new Fiber (scheduler)
  private var medics = Map.empty [MailboxId, Medic]
  private var makers = Map.empty [MailboxId, Disks => Poster]

  private def getMedic (id: MailboxId): Medic = {
    medics get (id) match {
      case Some (m) =>
        m
      case None =>
        val m = Medic()
        medics += id -> m
        m
    }}

  private def getMaker (id: MailboxId): Disks => Poster =
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

  def launch (implicit launch: Disks.Launch): Async [Catalogs] =
    fiber.supply {
      import launch.disks

      val handlers = Map.newBuilder [MailboxId, Handler]
      for (id <- medics.keySet ++ makers.keySet) yield {
        val poster = getMaker (id) .apply (disks)
        val handler = getMedic (id) .close (poster)
        handlers += id -> handler
      }
      val broker = new Broker (handlers.result)
      broker.attach (cluster)
      broker
    }}
