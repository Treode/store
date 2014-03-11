package com.treode.disk

import scala.language.implicitConversions

import com.google.common.primitives.UnsignedLongs

class TypeId private (val id: Long) extends AnyVal with Ordered [TypeId] {

  def compare (that: TypeId): Int =
    UnsignedLongs.compare (this.id, that.id)

  override def toString =
    if (id < 256) f"Type:$id%02X" else f"Type:$id%016X"
}

object TypeId extends Ordering [TypeId] {

  implicit def apply (id: Long): TypeId =
    new TypeId (id)

  def compare (x: TypeId, y: TypeId): Int =
    x compare y

  val pickler = {
    import DiskPicklers._
    wrap (fixedLong) build (new TypeId (_)) inspect (_.id)
  }}
