package com.treode.cluster

import scala.language.implicitConversions
import scala.util.Random
import com.treode.pickle.Picklers

class PortId (val id: Long) extends AnyVal with Ordered [PortId] {

  def isFixed = PortId.isFixed (this)

  def compare (that: PortId): Int =
    this.id compare that.id

  override def toString =
    if (id < 256) f"Port:$id%02X" else f"Port:$id%016X"
}

object PortId extends Ordering [PortId] {

  private val fixed = 0xFF00000000000000L

  implicit def apply (id: Long): PortId =
    new PortId (id)

  def isFixed (id: PortId): Boolean =
    (id.id & fixed) == fixed

  def isEphemeral (id: PortId): Boolean =
    (id.id & fixed) != fixed

  def newEphemeral(): PortId = {
    var id = Random.nextLong()
    while ((id & fixed) == fixed)
      id = Random.nextLong
    new PortId (id)
  }

  def compare (x: PortId, y: PortId): Int =
    x compare y

  val pickler = {
    import Picklers._
    wrap (fixedLong) build (apply _) inspect (_.id)
  }}
