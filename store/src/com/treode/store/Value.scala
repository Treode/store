package com.treode.store

import com.treode.pickle.Pickler

/** A value together with its timestamp; sorts in reverse chronological order.  If the value is 
  * `None`, that means the row was deleted at that timestamp.
  */
case class Value (time: TxClock, value: Option [Bytes]) {

  def value [V] (p: Pickler [V]): Option [V] =
    value map (_.unpickle (p))
}

object Value {

  val empty = Value (TxClock.MinValue, None)

  val pickler = {
    import StorePicklers._
    wrap (txClock, option (bytes))
    .build ((apply _).tupled)
    .inspect (v => (v.time, v.value))
  }}
