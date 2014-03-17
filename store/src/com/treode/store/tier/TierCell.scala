package com.treode.store.tier

import java.util.{Map => JMap}
import com.treode.store.{Bytes, StorePicklers}

class TierCell (val key: Bytes, val value: Option [Bytes]) extends Ordered [TierCell] {

  def byteSize = TierCell.pickler.byteSize (this)

  def compare (that: TierCell): Int = key compare that.key

  override def hashCode: Int = key.hashCode

  override def equals (other: Any) =
    other match {
      case that: TierCell => this.key == that.key
      case _ => false
    }

  override def toString = "TierCell" + (key, value)
}

object TierCell extends Ordering [TierCell] {

  def apply (key: Bytes, value: Option [Bytes]): TierCell =
    new TierCell (key, value)

  def apply (entry: JMap.Entry [Bytes, Option [Bytes]]): TierCell =
    new TierCell (entry.getKey, entry.getValue)

  def compare (x: TierCell, y: TierCell): Int =
    x compare y

  val pickler = {
    import StorePicklers._
    wrap (bytes, option (bytes))
    .build (v => new TierCell (v._1, v._2))
    .inspect (v => (v.key, v.value))
  }}
