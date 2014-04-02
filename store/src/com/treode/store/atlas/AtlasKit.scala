package com.treode.store.atlas

import com.treode.async.Async
import com.treode.cluster.{ReplyTracker, HostId}
import com.treode.store.{Atlas, Catalogs, CatalogDescriptor, Cohort, StorePicklers}
import com.treode.pickle.Pickler

import Async.supply
import Integer.highestOneBit

private [store] class AtlasKit extends Atlas {

  var cohorts = new Array [Cohort] (0)
  var mask = 0

  def locate (id: Int): Cohort =
    cohorts (id & mask)

  def issue (cohorts: Array [Cohort]) (implicit catalogs: Catalogs): Async [Unit] =
    catalogs.issue (AtlasKit.catalog) (1, cohorts)

  def set (cohorts: Array [Cohort]) {
    require (highestOneBit (cohorts.length) == cohorts.length)
    this.cohorts = cohorts
    this.mask = cohorts.length - 1
  }}

private [store] object AtlasKit {

  def recover (recovery: Catalogs.Recovery): Atlas = {
    val atlas = new AtlasKit
    recovery.listen (AtlasKit.catalog) (atlas.set)
    atlas
  }

  val catalog = {
    import StorePicklers._
    CatalogDescriptor (0x693799787FDC9106L, array (cohort))
  }}
