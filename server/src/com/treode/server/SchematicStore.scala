package com.treode.server

import com.treode.store._
import com.treode.async.Async
import com.treode.async.BatchIterator
import com.fasterxml.jackson.databind.JsonNode

class SchematicStore (store: Store, schema: Schema) {

  def read (name: String, key: String, rt: TxClock): Async [Seq [Value]] = {
    val ops = Seq (ReadOp (schema.getTableId  (name), Bytes (key)))
      store.read (rt, ops:_*)
  }

  def update (name: String, key: String, value: JsonNode, tx: TxId, ct: TxClock): Async [TxClock] = {
    val ops = Seq (WriteOp.Update (schema.getTableId (name), Bytes (key), value.toBytes))
    store.write (tx, ct, ops:_*)
  }

  def delete (name: String, key: String, tx: TxId, ct: TxClock): Async [TxClock] = {
    val ops = Seq (WriteOp.Delete (schema.getTableId (name), Bytes (key)))
    store.write (tx, ct, ops:_*)
  }

  def scan (
      name: String,
      key: Bound [Key] = Bound.firstKey,
      window: Window = Window.all,
      slice: Slice = Slice.all,
      batch: Batch = Batch.suggested
  ): BatchIterator [Cell] = {
    store.scan (schema.getTableId (name), key, window, slice, batch)
  }}
