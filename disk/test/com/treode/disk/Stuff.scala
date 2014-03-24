package com.treode.disk

import scala.util.Random

class Stuff (val seed: Long, val items: Seq [Int]) {

  override def equals (other: Any): Boolean =
    other match {
      case that: Stuff => seed == that.seed && items == that.items
      case _ => false
    }

  override def toString = f"Stuff(0x$seed%016X, 0x${items.hashCode}%08X)"
}

object Stuff {

  val countLimit = 100
  val valueLimit = Int.MaxValue

  def apply (seed: Long): Stuff = {
    val r = new Random (seed)
    val xs = Seq.fill (r.nextInt (countLimit)) (r.nextInt (valueLimit))
    new Stuff (seed, xs)
  }

  val pickler = {
    import DiskPicklers._
    wrap (fixedLong, seq (int))
    .build (v => new Stuff (v._1, v._2))
    .inspect (v => (v.seed, v.items))
  }}
