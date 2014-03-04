package com.treode.store.catalog

import com.treode.store.StorePicklers

private trait CatalogPicklers extends StorePicklers {

  def assign = Assign.pickler
  def patch = Patch.pickler
  def update = Update.pickler

  lazy val proposal = option (tuple (ballotNumber, patch))
}

private object CatalogPicklers extends CatalogPicklers
