package com.treode.store.local

import org.scalatest.FreeSpec

class TempTimedTableSpec extends FreeSpec with TimedTableBehaviors {

  def newStore: TestableLocalStore = new TestableTempLocalStore (4)

  "The TempTable" - {
    behave like aTimedTable
  }}
