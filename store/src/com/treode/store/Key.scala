package com.treode.store

import com.treode.pickle.Pickler

/** A key together with a timestamp; sorts in reverse chronological order. */
case class Key (key: Bytes, time: TxClock) extends Ordered [Key] {

  def key [K] (p: Pickler [K]): K =
    key.unpickle (p)

  def compare (that: Key): Int = {
    val r = key compare that.key
    if (r != 0) return r
    // Reverse chronological order
    that.time compare time
  }}

object Key extends Ordering [Key] {

  val MinValue = Key (Bytes.MinValue, TxClock.MaxValue)

  def apply [K] (pk: Pickler [K], key: K, time: TxClock): Key =
    Key (Bytes (pk, key), time)

  def compare (x: Key, y: Key): Int =
    x compare y

  val pickler = {
    import StorePicklers._
    wrap (bytes, txClock)
    .build (v => new Key (v._1, v._2))
    .inspect (v => (v.key, v.time))
  }}
