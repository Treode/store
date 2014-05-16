package com.treode.store.tier

import com.treode.disk.Position
import com.treode.store.{Residents, StoreConfig, StorePicklers}

 private case class Tiers (tiers: Seq [Tier]) {

  def apply (i: Int): Tier =
    tiers (i)

  def size: Int =
    tiers.length

  def isEmpty: Boolean =
    tiers.isEmpty

  def gen: Long =
    if (tiers.isEmpty) 0 else tiers.head.gen

  def keys: Long =
    tiers .map (_.keys) .sum

  def active: Set [Long] =
    tiers .map (_.gen) .toSet

  def choose (gens: Set [Long], residents: Residents) (implicit config: StoreConfig): Tiers = {
    var selected = -1
    var bytes = 0L
    var i = 0
    while (i < tiers.length) {
      val tier = tiers (i)
      if (gens contains tier.gen)
        selected = i
      if (tier.residents.exodus (residents))
        selected = i
      if (tier.diskBytes < bytes)
        selected = i
      bytes += tier.diskBytes
      i += 1
    }
    new Tiers (tiers take (selected + 1))
  }

  def compacted (tier: Tier, replace: Tiers): Tiers = {
    val keep = if (replace.tiers.isEmpty) Long.MaxValue else replace.tiers.map (_.gen) .min
    val bldr = Seq.newBuilder [Tier]
    bldr ++= tiers takeWhile (_.gen > tier.gen)
    bldr += tier
    bldr ++= tiers dropWhile (_.gen >= keep)
    new Tiers (bldr.result)
  }

  override def toString: String =
    s"Tiers(${tiers mkString ", "})"
}

private object Tiers {

  val empty: Tiers = new Tiers (Seq.empty)

  def apply (tier: Tier): Tiers =
    new Tiers (Array (tier))

  val pickler = {
    import StorePicklers._
    wrap (seq (Tier.pickler))
    .build (new Tiers (_))
    .inspect (_.tiers)
  }}
