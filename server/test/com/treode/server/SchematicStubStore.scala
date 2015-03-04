package com.treode.server

import com.treode.store.stubs.StubStore
import com.treode.store._

class SchematicStubStore (store: StubStore, schema: Schema) extends SchematicStore (store, schema) {

  def scan (name: String): Seq[Cell] = {
    store.scan (schema.getTableId (name))
  }}
