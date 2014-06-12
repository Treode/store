package com.treode.store.catalog

import scala.util.Random

import com.treode.async.{Async, Fiber, Scheduler}
import com.treode.async.implicits._
import com.treode.cluster.Cluster
import com.treode.disk.{Disk, Position}
import com.treode.store.{CatalogDescriptor, CatalogId, Library, StoreConfig}

import Async.guard

private class RecoveryKit (implicit
    random: Random,
    scheduler: Scheduler,
    library: Library,
    recovery: Disk.Recovery,
    config: StoreConfig
) extends Catalogs.Recovery {

  private val fiber = new Fiber
  private var medics = Map.empty [CatalogId, Medic]

  private def getMedic (id: CatalogId): Medic = {
    medics get (id) match {
      case Some (m) =>
        m
      case None =>
        val m = new Medic (id)
        medics += id -> m
        m
    }}

  Handler.update.replay { case (id, update) =>
    fiber.execute (getMedic (id) patch (update))
  }

  Handler.checkpoint.replay { case (id, meta) =>
    fiber.execute (getMedic (id) checkpoint (meta))
  }

  private def close (id: CatalogId) (implicit disk: Disk): Async [(CatalogId, Handler)] =
    for {
      handler <- getMedic (id) .close()
    } yield {
      (id, handler)
    }

  def launch (implicit launch: Disk.Launch, cluster: Cluster): Async [Catalogs] =
    fiber.guard {
      import launch.disk
      for {
        handlers <- medics.keySet.latch.map foreach (close (_))
        broker = new Broker (handlers)
        kit = new CatalogKit (broker)
      } yield {
        import kit.{acceptors, proposers}
        acceptors.attach()
        proposers.attach()
        broker.attach()
        kit
      }}}
