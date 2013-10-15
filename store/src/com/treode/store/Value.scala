package com.treode.store

import com.treode.pickle.Pickler

case class Value (time: TxClock, value: Option [Bytes]) {

  def value [V] (p: Pickler [V]): Option [V] =
    value map (_.unpickle (p))
}

object Value {

  val pickle = {
    import StorePicklers._
    wrap (txClock, option (bytes)) (apply _) (v => (v.time, v.value))
  }}
