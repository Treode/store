package com.treode.store.catalog

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.{Cluster, ReplyTracker}
import com.treode.disk.Disk
import com.treode.store._

private class CatalogKit (val broker: Broker) (implicit
    val random: Random,
    val scheduler: Scheduler,
    val cluster: Cluster,
    val disks: Disk,
    val library: Library,
    val config: StoreConfig
) extends Catalogs {

  val acceptors = new Acceptors (this)

  val proposers = new Proposers (this)

  def lead (key: CatalogId, patch: Patch): Async [Patch] =
    proposers.propose (0, key, patch)

  def propose (key: CatalogId, patch: Patch): Async [Patch] =
    proposers.propose (random.nextInt (17) + 1, key, patch)

  def listen [C] (desc: CatalogDescriptor [C]) (handler: C => Any): Unit =
    broker.listen (desc) (handler)

  def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Async [Unit] = {
    for {
      patch <- broker.diff (desc) (version, cat)
      chosen <- lead (desc.id, patch)
      _ <- broker.patch (desc.id, chosen)
    } yield {
      if (patch.checksum != chosen.checksum)
        throw new StaleException
    }}}
