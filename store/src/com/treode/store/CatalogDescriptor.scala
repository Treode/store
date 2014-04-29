package com.treode.store

import com.treode.async.Async
import com.treode.pickle.Pickler

class CatalogDescriptor [C] (val id: CatalogId, val pcat: Pickler [C]) {

  def listen (f: C => Any) (implicit store: Store.Controller): Unit =
    store.listen (this) (f)

  def issue (version: Int, cat: C) (implicit store: Store.Controller): Unit =
    store.issue (this) (version, cat)

  override def toString = s"CatalogDescriptor($id)"
}

object CatalogDescriptor {

  def apply [M] (id: CatalogId, pval: Pickler [M]): CatalogDescriptor [M] =
    new CatalogDescriptor (id, pval)
}
