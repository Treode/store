package com.treode.store.atlas

import com.treode.async.Async
import com.treode.cluster.{ReplyTracker, HostId}
import com.treode.store.{Atlas, Catalogs, CatalogDescriptor, Cohort, StorePicklers}
import com.treode.pickle.Pickler

import Async.supply
import Atlas.Recovery
import Integer.highestOneBit

private [store] class AtlasKit extends Atlas {

  var cohorts = new Array [Cohort] (0)
  var mask = 0

  def locate (id: Int): Cohort =
    cohorts (id & mask)

  def set (cohorts: Array [Cohort]) {
    require (highestOneBit (cohorts.length) == cohorts.length)
    this.cohorts = cohorts
    this.mask = cohorts.length - 1
  }

  def attach (recovery: Catalogs.Recovery) {
    recovery.listen (AtlasKit.catalog) (set _)
  }}

private [store] object AtlasKit {

  def recover (recovery: Catalogs.Recovery): Recovery = {
    val atlas = new AtlasKit
    atlas.attach (recovery)
    new Recovery {
      def launch(): Async [Atlas] =
        supply (atlas)
    }}

  val catalog = {
    import StorePicklers._
    CatalogDescriptor (0x693799787FDC9106L, array (cohort))
  }}
