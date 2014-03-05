package com.treode.store

import scala.language.implicitConversions
import com.treode.pickle.Picklers

class CatalogId (val id: Int) extends AnyVal

object CatalogId {

  implicit def apply (id: Int): CatalogId =
    new CatalogId (id)

  val pickler = {
    import Picklers._
    wrap (fixedInt) build (apply _) inspect (_.id)
  }}
