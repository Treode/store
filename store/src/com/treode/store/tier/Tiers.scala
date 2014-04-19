package com.treode.store.tier

import scala.collection.JavaConversions._
import com.treode.disk.Position
import com.treode.store.StorePicklers

 private class Tiers (val tiers: Array [Tier]) {

  def apply (i: Int): Tier =
    tiers (i)

  def gen: Long =
    if (tiers.isEmpty) 0L else tiers (0) .gen

  def active: Set [Long] =
    tiers .map (_.gen) .toSet

  def keys: Long =
    tiers .map (_.keys) .sum

  def size: Int =
    tiers.length

  def isEmpty: Boolean =
    tiers.length == 0

  override def toString: String =
    s"Tiers(${tiers mkString ", "})"
}

private object Tiers {

  val empty: Tiers = new Tiers (Array())

  def apply (tier: Tier): Tiers =
    new Tiers (Array (tier))

  val pickler = {
    import StorePicklers._
    wrap (array (Tier.pickler))
    .build (new Tiers (_))
    .inspect (_.tiers)
  }}
