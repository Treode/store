package com.treode.disk

import scala.language.implicitConversions

import com.treode.pickle.Picklers

class TypeId private (val id: Int) extends AnyVal {

  override def toString = f"Type:$id%04X"
}

object TypeId {

  implicit def apply (id: Int): TypeId =
    new TypeId (id)

  val pickle = {
    import Picklers._
    wrap (fixedInt) build (new TypeId (_)) inspect (_.id)
  }}
