package com.treode.store.local

import com.treode.pickle.size
import com.treode.store.{Bytes, StorePicklers, TxClock}

private class SimpleCell (val key: Bytes, val value: Option [Bytes])
extends Ordered [SimpleCell] {

  def byteSize = size (SimpleCell.pickle, this)

  def compare (that: SimpleCell): Int = key compare that.key

  override def hashCode: Int = key.hashCode

  override def equals (other: Any) =
    other match {
      case that: SimpleCell => this.key == that.key
      case _                => false
    }

  override def toString = "Cell" + (key, value)
}

private object SimpleCell extends Ordering [SimpleCell] {

  def apply (key: Bytes, value: Option [Bytes]): SimpleCell =
    new SimpleCell (key, value)

  def compare (x: SimpleCell, y: SimpleCell): Int =
    x compare y

  val pickle = {
    import StorePicklers._
    wrap2 (bytes, option (bytes)) (SimpleCell.apply _) (v => (v.key, v.value))
  }}
