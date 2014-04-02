package com.treode.store.catalog

import scala.util.Random

import com.treode.async.{Async, Scheduler}
import com.treode.cluster.{Cluster, ReplyTracker}
import com.treode.disk.Disks
import com.treode.store.{Atlas, Bytes, Catalogs, CatalogDescriptor, CatalogId, StoreConfig}

private class CatalogKit (val broker: Broker) (implicit
    val random: Random,
    val scheduler: Scheduler,
    val cluster: Cluster,
    val disks: Disks,
    val atlas: Atlas,
    val config: StoreConfig
) extends Catalogs {

  val acceptors = new Acceptors (this)

  val proposers = new Proposers (this)

  def locate(): ReplyTracker =
    atlas.locate (0) .track

  def lead (key: CatalogId, patch: Patch): Async [Update] =
    proposers.propose (0, key, patch)

  def propose (key: CatalogId, patch: Patch): Async [Update] =
    proposers.propose (random.nextInt (17) + 1, key, patch)

  def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Async [Unit] = {
    for {
      patch <- broker.diff (desc) (version, cat)
      chosen <- lead (desc.id, patch)
      _ <- broker.patch (desc.id, chosen)
    } yield {
      require (
          patch.checksum == chosen.checksum,
          "Could not propose new issue for version $version; it was already issued.")
    }}
}

private [store] object CatalogKit {

  def recover () (implicit
      random: Random,
      scheduler: Scheduler,
      cluster: Cluster,
      recovery: Disks.Recovery,
      config: StoreConfig): Catalogs.Recovery =
    new RecoveryKit
}
