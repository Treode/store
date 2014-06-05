package com.treode.store

case class Key (key: Bytes, time: TxClock) extends Ordered [Key] {

  def compare (that: Key): Int = {
    val r = key compare that.key
    if (r != 0) return r
    // Reverse chronological order
    that.time compare time
  }}

object Key extends Ordering [Key] {

  val MinValue = Key (Bytes.MinValue, TxClock.MaxValue)

  def compare (x: Key, y: Key): Int =
    x compare y

  val pickler = {
    import StorePicklers._
    wrap (bytes, txClock)
    .build (v => new Key (v._1, v._2))
    .inspect (v => (v.key, v.time))
  }}
