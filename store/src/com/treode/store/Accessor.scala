package com.treode.store

import com.treode.pickle.Pickler

import WriteOp.{Create, Delete, Hold, Update}

class Accessor [K, V] (id: TableId, pk: Pickler [K], pv: Pickler [V]) {

  def read (k: K)         = ReadOp (id, Bytes (pk, k))
  def value (v: Value)    = v.value (pv)
  def create (k: K, v: V) = Create (id, Bytes (pk, k), Bytes (pv, v))
  def hold (k: K)         = Hold (id, Bytes (pk, k))
  def update (k: K, v: V) = Update (id, Bytes (pk, k), Bytes (pv, v))
  def delete (k: K)       = Delete (id, Bytes (pk, k))
}
