package com.treode.disk

import scala.language.implicitConversions

import com.google.common.primitives.UnsignedLongs

class CellId private (val id: Long) extends AnyVal with Ordered [CellId] {

  def compare (that: CellId): Int =
    UnsignedLongs.compare (this.id, that.id)

  override def toString =
    if (id < 256) f"Cell:$id%02X" else f"Cell:$id%016X"
}

object CellId extends Ordering [CellId] {

  implicit def apply (id: Long): CellId =
    new CellId (id)

  def compare (x: CellId, y: CellId): Int =
    x compare y

  val pickler = {
    import DiskPicklers._
    wrap (fixedLong) build (new CellId (_)) inspect (_.id)
  }}
