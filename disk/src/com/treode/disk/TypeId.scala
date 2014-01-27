package com.treode.disk

import scala.language.implicitConversions

class TypeId private (val id: Int) extends AnyVal {

  override def toString = f"Type:$id%04X"
}

object TypeId {

  implicit def apply (id: Int): TypeId =
    new TypeId (id)

  val pickler = {
    import DiskPicklers._
    wrap (fixedInt) build (new TypeId (_)) inspect (_.id)
  }}
