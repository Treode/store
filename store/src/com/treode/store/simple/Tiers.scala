package com.treode.store.simple

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

  def size: Int =
    tiers.length

  def isEmpty: Boolean =
    tiers.length == 0
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
