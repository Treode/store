package com.treode.store

private trait SimpleStore {

  def table (id: TableId): SimpleTable
}
