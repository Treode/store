package com.treode.store

import com.treode.pickle.Pickler

case class Value (time: TxClock, value: Option [Bytes]) {

  def value [V] (p: Pickler [V]): Option [V] =
    value map (_.unpickle (p))
}

object Value {

  val empty = Value (TxClock.zero, None)

  val pickle = {
    import StorePicklers._
    wrap (txClock, option (bytes))
    .build ((apply _).tupled)
    .inspect (v => (v.time, v.value))
  }}
