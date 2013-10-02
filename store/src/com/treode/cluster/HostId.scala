package com.treode.cluster

import scala.language.implicitConversions

import com.treode.pickle.Picklers

class HostId private (val id: Long) extends Ordered [HostId] {

  def compare (that: HostId): Int =
    this.id compare that.id

  override def hashCode = id.hashCode

  override def equals (other: Any) =
    other match {
      case that: HostId => this.id == that.id
      case _ => false
    }

  override def toString = "Host:%08X" format id
}

object HostId extends Ordering [HostId] {

  implicit def apply (id: Long): HostId =
    new HostId (id)

  val pickle = {
    import Picklers._
    wrap [Long, HostId] (fixedLong, HostId (_), _.id)
  }

  def compare (x: HostId, y: HostId): Int =
    x compare y
}
