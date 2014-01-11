package com.treode.store

import scala.language.implicitConversions

class TxId private (val id: Bytes) extends AnyVal {

  override def toString = f"TxId:$id"
}

object TxId {

  implicit def apply (id: Bytes): TxId =
    new TxId (id)

  implicit def apply (id: Long): TxId =
    new TxId (Bytes (id))

  val pickle = {
    import StorePicklers._
    wrap (bytes) build (apply _) inspect (_.id)
  }}
