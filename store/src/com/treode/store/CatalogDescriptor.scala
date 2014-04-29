package com.treode.store

import com.treode.async.Async
import com.treode.pickle.Pickler

class CatalogDescriptor [C] (val id: CatalogId, val pcat: Pickler [C]) {

  override def toString = s"CatalogDescriptor($id)"
}

object CatalogDescriptor {

  def apply [M] (id: CatalogId, pval: Pickler [M]): CatalogDescriptor [M] =
    new CatalogDescriptor (id, pval)
}
