package com.treode.store.atlas

import com.treode.async.Async
import com.treode.cluster.HostId
import com.treode.store.{Atlas, Catalogs, CatalogDescriptor, StorePicklers}

import Async.supply
import Atlas.Recovery

private class AtlasKit extends Atlas {

  var places = new Array [Seq [HostId]] (0)
  var mask = 0

  def attach (recovery: Catalogs.Recovery) {
    recovery.listen (AtlasKit.catalog) { places =>
      require (Integer.highestOneBit (places.length) == places.length)
      this.places = places
      this.mask = places.length - 1
    }}}

private [store] object AtlasKit {

  def recover (recovery: Catalogs.Recovery): Recovery = {
    val cohorts = new AtlasKit
    cohorts.attach (recovery)
    new Recovery {
      def launch(): Async [Atlas] =
        supply (cohorts)
    }}

  val catalog = {
    import StorePicklers._
    CatalogDescriptor (0x65F723E9, array (seq (hostId)))
  }}
