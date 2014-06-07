package com.treode.store

import com.treode.async.Async
import com.treode.store.catalog.Catalogs

private class ControllerAgent (
    catalogs: Catalogs,
    librarian: Librarian,
    val store: Store
) extends Store.Controller {

  def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any): Unit =
    catalogs.listen (desc) (f)

  def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Async [Unit] =
    catalogs.issue (desc) (version, cat)

  def rebalance (cohorts: Seq [Cohort]): Unit =
    librarian.issueAtlas (cohorts.toArray)
}
