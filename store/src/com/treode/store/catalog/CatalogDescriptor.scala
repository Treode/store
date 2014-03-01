package com.treode.store.catalog

import com.treode.cluster.MailboxId
import com.treode.pickle.{Pickler, Picklers}

class CatalogDescriptor [C] (val id: MailboxId, val pcat: Pickler [C]) {

  def listen (f: C => Any): Unit =
    ???

  def issue (cat: C): Unit =
    ???

  override def toString = s"CatalogDescriptor($id,$pcat)"
}

object CatalogDescriptor {

  def apply [M] (id: MailboxId, pval: Pickler [M]): CatalogDescriptor [M] =
    new CatalogDescriptor (id, pval)
}
