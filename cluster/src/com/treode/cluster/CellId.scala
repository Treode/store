package com.treode.cluster

import scala.language.implicitConversions

import com.google.common.primitives.UnsignedLongs
import com.treode.pickle.Picklers

class CellId private (val id: Long) extends AnyVal with Ordered [CellId] {

  def compare (that: CellId): Int =
    UnsignedLongs.compare (this.id, that.id)

  override def toString =
    if (id < 256) f"Cell:$id%02X" else f"Cell:$id%016X"
}

object CellId extends Ordering [CellId] {

  val MinValue = CellId (0)

  val MaxValue = CellId (-1)

  implicit def apply (id: Long): CellId =
    new CellId (id)

  def compare (x: CellId, y: CellId): Int =
    x compare y

  val pickler = {
    import Picklers._
    wrap (fixedLong) build (apply _) inspect (_.id)
  }}
