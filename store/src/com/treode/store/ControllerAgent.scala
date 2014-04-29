package com.treode.store

import com.treode.store.catalog.Catalogs

private class ControllerAgent (catalogs: Catalogs, val store: Store) extends Store.Controller {

  def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any): Unit =
    catalogs.listen (desc) (f)

  def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Unit =
    catalogs.issue (desc) (version, cat)

  def issue (atlas: Atlas): Unit =
    catalogs.issue (Atlas.catalog) (atlas.version - 1, atlas)
}
