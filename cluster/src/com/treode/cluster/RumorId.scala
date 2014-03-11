package com.treode.cluster

import scala.language.implicitConversions

import com.google.common.primitives.UnsignedLongs
import com.treode.pickle.Picklers

class RumorId (val id: Long) extends AnyVal with Ordered [RumorId] {

  def compare (that: RumorId): Int =
    UnsignedLongs.compare (this.id, that.id)

  override def toString =
    if (id < 256) f"Rumor:$id%02X" else f"Rumor:$id%016X"
}

object RumorId extends Ordering [RumorId] {

  implicit def apply (id: Long): RumorId =
    new RumorId (id)

  def compare (x: RumorId, y: RumorId): Int =
    x compare y

  val pickler = {
    import Picklers._
    wrap (fixedLong) build (apply _) inspect (_.id)
  }}
