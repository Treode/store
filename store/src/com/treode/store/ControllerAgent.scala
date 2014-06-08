package com.treode.store

import com.treode.async.Async
import com.treode.store.catalog.Catalogs

private class ControllerAgent (
    library: Library,
    librarian: Librarian,
    catalogs: Catalogs,
    val store: Store
) extends Store.Controller {

  def cohorts: Seq [Cohort] =
    library.atlas.cohorts.toSeq

  def cohorts_= (v: Seq [Cohort]): Unit =
    librarian.issueAtlas (v.toArray)

  def listen [C] (desc: CatalogDescriptor [C]) (f: C => Any): Unit =
    catalogs.listen (desc) (f)

  def issue [C] (desc: CatalogDescriptor [C]) (version: Int, cat: C): Async [Unit] =
    catalogs.issue (desc) (version, cat)
}
