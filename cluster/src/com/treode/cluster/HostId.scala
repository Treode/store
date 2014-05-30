package com.treode.cluster

import scala.language.implicitConversions

import com.google.common.primitives.UnsignedLongs
import com.treode.pickle.Picklers

class HostId private (val id: Long) extends AnyVal with Ordered [HostId] {

  def compare (that: HostId): Int =
    UnsignedLongs.compare (this.id, that.id)

  override def toString =
    if (id < 256) f"Host:$id%02X" else f"Host:$id%016X"
}

object HostId extends Ordering [HostId] {

  val MinValue = HostId (0)

  val MaxValue = HostId (-1)

  implicit def apply (id: Long): HostId =
    new HostId (id)

  def compare (x: HostId, y: HostId): Int =
    x compare y

  val pickler = {
    import Picklers._
    wrap (fixedLong) build (apply _) inspect (_.id)
  }}
