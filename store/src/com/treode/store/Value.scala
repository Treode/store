package com.treode.store

import com.treode.pickle.Pickler

case class Value (time: TxClock, value: Option [Bytes]) {

  def value [V] (p: Pickler [V]): Option [V] =
    value map (_.unpickle (p))
}

object Value {

  val pickle = {
    import StorePicklers._
    wrap [(TxClock, Option [Bytes]), Value] (
        tuple (txClock, option (bytes)),
        (v => Value (v._1, v._2)),
        (v => (v.time, v.value)))
  }}
