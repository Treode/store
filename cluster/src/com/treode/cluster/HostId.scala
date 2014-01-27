package com.treode.cluster

import scala.language.implicitConversions

import com.treode.pickle.Picklers

class HostId private (val id: Long) extends AnyVal with Ordered [HostId] {

  def compare (that: HostId): Int =
    this.id compare that.id

  override def toString = f"Host:$id%016X"
}

object HostId extends Ordering [HostId] {

  implicit def apply (id: Long): HostId =
    new HostId (id)

  val pickler = {
    import Picklers._
    wrap (fixedLong) build (apply _) inspect (_.id)
  }

  def compare (x: HostId, y: HostId): Int =
    x compare y
}
