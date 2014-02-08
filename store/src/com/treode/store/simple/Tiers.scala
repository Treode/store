package com.treode.store.simple

import scala.collection.JavaConversions._
import com.treode.disk.Position
import com.treode.store.StorePicklers

 private class Tiers (val tiers: Array [Tier]) {

  def pos (i: Int) = tiers (i) .pos

  def active = tiers .map (_.gen) .toSet

  def size = tiers.length
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
