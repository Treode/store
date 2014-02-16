package com.treode.store

import com.treode.store._

private trait TestableLocalKit extends TestableLocalStore {
  this: LocalKit =>

  def getTimedTable (id: TableId): TimedTable
}
