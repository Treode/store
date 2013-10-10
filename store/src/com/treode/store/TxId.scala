package com.treode.store

import scala.language.implicitConversions

import com.treode.pickle.Picklers

class TxId private (val id: Bytes) extends AnyVal {

  override def toString = f"TxId:$id"
}

object TxId {

  implicit def apply (id: Bytes): TxId =
    new TxId (id)

  // Supports testing only.
  private [store] implicit def apply (id: Int): TxId =
    new TxId (Bytes (Picklers.int, id))

  val pickle = {
    import Picklers._
    wrap [Bytes, TxId] (Bytes.pickle, TxId (_), _.id)
  }}
