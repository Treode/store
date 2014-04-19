package com.treode.store.atlas

import scala.util.{Failure, Success}

import com.treode.async.Async
import com.treode.cluster.{Cluster, RumorDescriptor}
import com.treode.store.{Atlas, Catalogs, CatalogDescriptor, Cohort, Residents, StorePicklers}

import Async.{guard, supply}
import AtlasKit.catalog
import Integer.highestOneBit

private [store] class AtlasKit (implicit cluster: Cluster) extends Atlas {
  import cluster.localId

  private val rebalancers = new RebalanceRegistry

  var version = 0
  var cohorts = new Array [Cohort] (0)
  var mask = 0
  var residents = Residents.empty

  def rebalance (f: Array [Cohort] => Async [Unit]): Unit =
    rebalancers.rebalance (f)

  def rebalance (version: Int, cohorts: Array [Cohort]): Unit =
    guard {
      rebalancers.rebalance (cohorts)
    } .run {
      case Success (_) => AtlasKit.version.spread (version)
      case Failure (t) => throw t
    }

  def locate (id: Int): Cohort =
    cohorts (id & mask)

  def issue (cohorts: Array [Cohort]) (implicit catalogs: Catalogs): Async [Unit] =
    catalogs.issue (catalog) (1, (1, cohorts))

  def set (version: Int, cohorts: Array [Cohort]) {
    require (highestOneBit (cohorts.length) == cohorts.length)
    this.version = version
    this.cohorts = cohorts
    this.mask = cohorts.length - 1
    this.residents = Residents (localId, cohorts)
    rebalance (version, cohorts)
  }}

private [store] object AtlasKit {

  def recover () (implicit cluster: Cluster, catalogs: Catalogs.Recovery): Atlas = {
    val atlas = new AtlasKit
    catalog.listen ((atlas.set _).tupled)
    atlas
  }

  val catalog = {
    import StorePicklers._
    CatalogDescriptor (0x693799787FDC9106L, tuple (uint, array (cohort)))
  }

  val version = {
    import StorePicklers._
    RumorDescriptor.apply (0x03C199E74563DBD6L, uint)
  }}
