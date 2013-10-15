package com.treode.store

import scala.language.implicitConversions

class TxId private (val id: Bytes) extends AnyVal {

  override def toString = f"TxId:$id"
}

object TxId {

  implicit def apply (id: Bytes): TxId =
    new TxId (id)

  // Supports testing only.
  private [store] implicit def apply (id: Int): TxId =
    new TxId (Bytes (StorePicklers.int, id))

  val pickle = {
    import StorePicklers._
    wrap (bytes) (apply _) (_.id)
  }}
