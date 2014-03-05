package com.treode.cluster

import scala.language.implicitConversions
import scala.util.Random
import com.treode.pickle.Picklers

class PortId (val id: Long) extends AnyVal {

  def isFixed = PortId.isFixed (id)
  override def toString = f"Mailbox:$id%016X"
}

object PortId {

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

  val pickler = {
    import Picklers._
    wrap (fixedLong) build (apply _) inspect (_.id)
  }}
