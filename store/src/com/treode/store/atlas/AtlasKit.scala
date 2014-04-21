package com.treode.store.atlas

import scala.util.{Failure, Success}

import com.treode.async.Async
import com.treode.cluster.{Cluster, Peer, RumorDescriptor}
import com.treode.store._

import Async.{guard, supply}
import AtlasKit.catalog
import Integer.highestOneBit

private [store] class AtlasKit (implicit cluster: Cluster) extends Atlas {
  import cluster.localId

  private val rebalancers = new RebalanceRegistry

  var cohorts = Cohorts.empty
  var residents = Residents.all

  def rebalance (f: Cohorts => Async [Unit]): Unit =
    rebalancers.rebalance (f)

  def rebalance (cohorts: Cohorts): Unit =
    guard {
      rebalancers.rebalance (cohorts)
    } .run {
      case Success (_) => AtlasKit.version.spread (cohorts.version)
      case Failure (t) => throw t
    }

  def place (id: Int): Int =
    cohorts.place (id)

  def locate (id: Int): Cohort =
    cohorts.locate (id)

  def issue (cohorts: Cohorts) (implicit catalogs: Catalogs): Async [Unit] =
    catalogs.issue (catalog) (1, cohorts)

  def set (cohorts: Cohorts) {
    this.cohorts = cohorts
    this.residents = Residents (localId, cohorts.cohorts)
    rebalance (cohorts)
  }}

private [store] object AtlasKit {

  def recover () (implicit cluster: Cluster, catalogs: Catalogs.Recovery): Atlas = {
    val atlas = new AtlasKit
    catalog.listen (atlas.set _)
    atlas
  }

  val catalog = {
    import StorePicklers._
    CatalogDescriptor (0x693799787FDC9106L, cohorts)
  }

  val version = {
    import StorePicklers._
    RumorDescriptor.apply (0x03C199E74563DBD6L, uint)
  }}
