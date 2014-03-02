package com.treode.store.catalog

import com.treode.async.Async
import com.treode.cluster.HostId
import com.treode.store.StorePicklers

import Async.supply

class CohortCatalog {

  var places = new Array [Seq [HostId]] (0)
  var mask = 0

  def attach (recovery: Catalogs.Recovery) {
    recovery.listen (CohortCatalog.catalog) { places =>
      require (Integer.highestOneBit (places.length) == places.length)
      this.places = places
      this.mask = places.length - 1
    }}}

object CohortCatalog {

  trait Recovery {
    def launch(): Async [CohortCatalog]
  }

  def recover (recovery: Catalogs.Recovery): Recovery = {
    val cohorts = new CohortCatalog
    cohorts.attach (recovery)
    new Recovery {
      def launch(): Async [CohortCatalog] =
        supply (cohorts)
    }}

  val catalog = {
    import StorePicklers._
    CatalogDescriptor (0x65F723E9, array (seq (hostId)))
  }}
