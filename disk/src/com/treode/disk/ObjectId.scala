package com.treode.disk

import scala.language.implicitConversions

import com.treode.pickle.Picklers

class ObjectId private (val id: Long) extends AnyVal with Ordered [ObjectId] {

  def compare (that: ObjectId): Int =
    this.id compare that.id

  override def toString =
    if (id < 256) f"Object:$id%02X" else f"Object:$id%016X"
}

object ObjectId extends Ordering [ObjectId] {

  implicit def apply (id: Long): ObjectId =
    new ObjectId (id)

  def compare (x: ObjectId, y: ObjectId): Int =
    x compare y

  val pickler = {
    import Picklers._
    wrap (fixedLong) build (apply _) inspect (_.id)
  }}
