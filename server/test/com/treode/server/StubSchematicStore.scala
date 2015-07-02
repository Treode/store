package com.treode.server

import scala.util.Random

import com.fasterxml.jackson.databind.JsonNode
import com.treode.async.{Async, Scheduler}
import com.treode.store.stubs.StubStore
import com.treode.store.{Bytes, Cell, TableId, TxClock, TxId, WriteOp}

class StubSchematicStore (store: StubStore, schema: Schema) {

  private def randomTx: TxId =
    TxId (Bytes (Random.nextInt), 0)

  private def requireTableId (name: String): TableId = {
    val id = schema.getTableId (name)
    require (id.isDefined, s"""No table named "$name".""")
    id.get
  }

  def update (tx: TxId, ct: TxClock, tab: TableId, key: String, value: JsonNode): Async [TxClock] =
    store.write (tx, ct, WriteOp.Update (tab, Bytes (key), value.toBytes))

  def update (ct: TxClock, tab: String, key: String, value: String): Async [TxClock] =
    update (randomTx, ct, requireTableId (tab), key, value.fromJson [JsonNode])

  def scan (name: String): Seq [Cell] = {
    store.scan (schema.getTableId (name) .get)
  }}
