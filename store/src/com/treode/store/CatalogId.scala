package com.treode.store

import scala.language.implicitConversions
import com.treode.pickle.Picklers

class CatalogId (val id: Long) extends AnyVal with Ordered [CatalogId] {

  def compare (that: CatalogId): Int =
    this.id compare that.id

  override def toString =
    if (id < 256) f"Catalog:$id%02X" else f"Catalog:$id%016X"
}

object CatalogId extends Ordering [CatalogId] {

  implicit def apply (id: Long): CatalogId =
    new CatalogId (id)

  def compare (x: CatalogId, y: CatalogId): Int =
    x compare y

  val pickler = {
    import Picklers._
    wrap (fixedLong) build (apply _) inspect (_.id)
  }}
